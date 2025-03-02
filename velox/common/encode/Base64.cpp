/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "velox/common/encode/Base64.h"

#include <folly/Portability.h>
#include <folly/container/Foreach.h>
#include <folly/io/Cursor.h>
#include <stdint.h>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::encoding {

// Constants defining the size in bytes of binary and encoded blocks for Base64
// encoding.
// Size of a binary block in bytes (3 bytes = 24 bits)
constexpr static int kBinaryBlockByteSize = 3;
// Size of an encoded block in bytes (4 bytes = 24 bits)
constexpr static int kEncodedBlockByteSize = 4;

// Character sets for Base64 and Base64 URL encoding
constexpr const Base64::Charset kBase64Charset = {
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
    'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
    'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'};

constexpr const Base64::Charset kBase64UrlCharset = {
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
    'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
    'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-', '_'};

// Validate the character in charset with ReverseIndex table
constexpr bool checkForwardIndex(
    uint8_t index,
    const Base64::Charset& charset,
    const Base64::ReverseIndex& reverseIndex) {
  return (reverseIndex[static_cast<uint8_t>(charset[index])] == index) &&
      (index > 0 ? checkForwardIndex(index - 1, charset, reverseIndex) : true);
}

// Verify that for every entry in kBase64Charset, the corresponding entry
// in kBase64ReverseIndexTable is correct.
static_assert(
    checkForwardIndex(
        sizeof(kBase64Charset) - 1,
        kBase64Charset,
        Base64::kBase64ReverseIndexTable),
    "kBase64Charset has incorrect entries");

// Verify that for every entry in kBase64UrlCharset, the corresponding entry
// in kBase64UrlReverseIndexTable is correct.
static_assert(
    checkForwardIndex(
        sizeof(kBase64UrlCharset) - 1,
        kBase64UrlCharset,
        Base64::kBase64UrlReverseIndexTable),
    "kBase64UrlCharset has incorrect entries");

// Searches for a character within a charset up to a certain index.
constexpr bool findCharacterInCharset(
    const Base64::Charset& charset,
    uint8_t index,
    const char targetChar) {
  return index < charset.size() &&
      ((charset[index] == targetChar) ||
       findCharacterInCharset(charset, index + 1, targetChar));
}

// Checks the consistency of a reverse index mapping for a given character
// set.
constexpr bool checkReverseIndex(
    uint8_t index,
    const Base64::Charset& charset,
    const Base64::ReverseIndex& reverseIndex) {
  return (reverseIndex[index] == 255
              ? !findCharacterInCharset(charset, 0, static_cast<char>(index))
              : (charset[reverseIndex[index]] == index)) &&
      (index > 0 ? checkReverseIndex(index - 1, charset, reverseIndex) : true);
}

// Verify that for every entry in kBase64ReverseIndexTable, the corresponding
// entry in kBase64Charset is correct.
static_assert(
    checkReverseIndex(
        sizeof(Base64::kBase64ReverseIndexTable) - 1,
        kBase64Charset,
        Base64::kBase64ReverseIndexTable),
    "kBase64ReverseIndexTable has incorrect entries.");

// Verify that for every entry in kBase64ReverseIndexTable, the corresponding
// entry in kBase64Charset is correct.
// We can't run this check as the URL version has two duplicate entries so
// that the url decoder can handle url encodings and default encodings
// static_assert(
//     checkReverseIndex(
//         sizeof(kBase64UrlReverseIndexTable) - 1,
//         kBase64UrlCharset,
//         kBase64UrlReverseIndexTable),
//     "kBase64UrlReverseIndexTable has incorrect entries.");

// Implementation of Base64 encoding and decoding functions.
// static
template <class T>
std::string Base64::encodeImpl(
    const T& input,
    const Charset& charset,
    bool includePadding,
    bool chunkBase64) {
  size_t encodedSize = calculateEncodedSize(input.size(), includePadding, chunkBase64);
  std::string encodedResult;
  encodedResult.resize(encodedSize);
  encodeImpl(input, charset, includePadding, encodedResult.data(), chunkBase64);
  return encodedResult;
}

// static
size_t Base64::calculateEncodedSize(size_t inputSize, bool withPadding, bool chunkBase64) {
  if (inputSize == 0) {
    return 0;
  }

  // Calculate the output size assuming that we are including padding.
  size_t encodedSize = ((inputSize + 2) / 3) * 4;
  if (!withPadding) {
    // If the padding was not requested, subtract the padding bytes.
    encodedSize -= (3 - (inputSize % 3)) % 3;
  }
  if (chunkBase64) {
    // For "\r\n".
    encodedSize += (encodedSize - 1) / 76 * 2;
  }
  return encodedSize;
}

// static
void Base64::encode(const char* input, size_t inputSize, char* output, bool chunkBase64) {
  encodeImpl(
      folly::StringPiece(input, inputSize), kBase64Charset, true, output, chunkBase64);
}

// static
void Base64::encodeUrl(const char* input, size_t inputSize, char* output) {
  encodeImpl(
      folly::StringPiece(input, inputSize),
      kBase64UrlCharset,
      true,
      output,
      true);
}

// static
template <class T>
void Base64::encodeImpl(
    const T& input,
    const Charset& charset,
    bool includePadding,
    char* outputBuffer,
    bool chunkBase64) {
  auto inputSize = input.size();
  if (inputSize == 0) {
    return;
  }

  auto outputPointer = outputBuffer;
  auto inputIterator = input.begin();

  // For each group of 3 bytes (24 bits) in the input, split that into
  // 4 groups of 6 bits and encode that using the supplied charset lookup
  unsigned char sum = 0;
  for (; inputSize > 2;) {
    uint32_t inputBlock = static_cast<uint8_t>(*inputIterator++) << 16;
    inputBlock |= static_cast<uint8_t>(*inputIterator++) << 8;
    inputBlock |= static_cast<uint8_t>(*inputIterator++);

    *outputPointer++ = charset[(inputBlock >> 18) & 0x3f];
    *outputPointer++ = charset[(inputBlock >> 12) & 0x3f];
    *outputPointer++ = charset[(inputBlock >> 6) & 0x3f];
    *outputPointer++ = charset[inputBlock & 0x3f];

    inputSize -= 3;

    if (chunkBase64) {
      sum += 3;
      if (sum == 57 && inputSize > 0) {
        sum = 0;
        *outputPointer++ = '\r';
        *outputPointer++ = '\n';
      }
    }
  }

  if (inputSize > 0) {
    // We have either 1 or 2 input bytes left.  Encode this similar to the
    // above (assuming 0 for all other bytes).  Optionally append the '='
    // character if it is requested.
    uint32_t inputBlock = static_cast<uint8_t>(*inputIterator++) << 16;
    *outputPointer++ = charset[(inputBlock >> 18) & 0x3f];
    if (inputSize > 1) {
      inputBlock |= static_cast<uint8_t>(*inputIterator) << 8;
      *outputPointer++ = charset[(inputBlock >> 12) & 0x3f];
      *outputPointer++ = charset[(inputBlock >> 6) & 0x3f];
      if (includePadding) {
        *outputPointer = kPadding;
      }
    } else {
      *outputPointer++ = charset[(inputBlock >> 12) & 0x3f];
      if (includePadding) {
        *outputPointer++ = kPadding;
        *outputPointer = kPadding;
      }
    }
  }
}

// static
std::string Base64::encode(folly::StringPiece text, bool chunkBase64) {
  return encodeImpl(text, kBase64Charset, true, chunkBase64);
}

// static
std::string Base64::encode(const char* input, size_t inputSize, bool chunkBase64) {
  return encode(folly::StringPiece(input, inputSize), chunkBase64);
}

namespace {

// This is a quick and simple iterator implementation for an IOBuf so that the
// template that uses iterators can work on IOBuf chains. It only implements
// postfix increment because that is all the algorithm needs, and it is a no-op
// since the read<>() function already increments the cursor.
class IOBufWrapper {
 private:
  class Iterator {
   public:
    explicit Iterator(const folly::IOBuf* inputBuffer) : cursor_(inputBuffer) {}

    Iterator& operator++(int32_t) {
      // This is a no-op since reading from the Cursor has already moved the
      // position.
      return *this;
    }

    uint8_t operator*() {
      // This will read _and_ increment the cursor.
      return cursor_.read<uint8_t>();
    }

   private:
    folly::io::Cursor cursor_;
  };

 public:
  explicit IOBufWrapper(const folly::IOBuf* inputBuffer)
      : input_(inputBuffer) {}
  size_t size() const {
    return input_->computeChainDataLength();
  }

  Iterator begin() const {
    return Iterator(input_);
  }

 private:
  const folly::IOBuf* input_;
};

} // namespace

// static
std::string Base64::encode(const folly::IOBuf* inputBuffer, bool chunkBase64) {
  return encodeImpl(IOBufWrapper(inputBuffer), kBase64Charset, true, chunkBase64);
}

// static
std::string Base64::decode(folly::StringPiece encodedText) {
  std::string decodedResult;
  Base64::decode(
      std::make_pair(encodedText.data(), encodedText.size()), decodedResult);
  return decodedResult;
}

// static
void Base64::decode(
    const std::pair<const char*, int32_t>& payload,
    std::string& decodedOutput) {
  size_t inputSize = payload.second;
  decodedOutput.resize(calculateDecodedSize(payload.first, inputSize, kBase64ReverseIndexTable));
  decode(payload.first, inputSize, decodedOutput.data(), decodedOutput.size());
}

// static
void Base64::decode(const char* input, size_t size, char* output) {
  size_t expectedOutputSize = size / 4 * 3;
  Base64::decode(input, size, output, expectedOutputSize);
}

// static
uint8_t Base64::base64ReverseLookup(
    char encodedChar,
    const Base64::ReverseIndex& reverseIndex) {
  auto reverseLookupValue = reverseIndex[static_cast<uint8_t>(encodedChar)];
  if (reverseLookupValue >= 0x40) {
    VELOX_USER_FAIL("decode() - invalid input string: invalid characters");
  }
  return reverseLookupValue;
}

// static
uint8_t Base64::tryBase64ReverseLookup(
    const char*& input,
    const Base64::ReverseIndex& reverseIndex) {
  while (true) {
    auto reverseLookupValue = reverseIndex[static_cast<uint8_t>(*input)];
    input++;
    if (reverseLookupValue >= 0x40) {
      continue;
    } else {
      return reverseLookupValue;
    }
  }
}

// static
size_t Base64::decode(
    const char* input,
    size_t inputSize,
    char* output,
    size_t outputSize) {
  return decodeImpl(
      input, inputSize, output, outputSize, kBase64ReverseIndexTable);
}

// static
size_t Base64::calculateDecodedSize(
    const char* input,
    size_t& inputSize,
    const ReverseIndex& reverseIndex) {
  if (inputSize == 0) {
    return 0;
  }

  size_t filteredInputSize = 0;
  size_t paddingSize = 0;
  for (int i = 0; i < inputSize; i++) {
    char c = input[i];
    if (c == kPadding) {
      i++;
      paddingSize++;
      while (i < inputSize) {
        if (input[i] != kPadding) {
          VELOX_USER_FAIL(
              "Base64::decode() - the characters after the padding symbol must all be padding.");
        }
        i++;
        paddingSize++;
      }
    } else {
      auto reverseLookupValue = reverseIndex[static_cast<uint8_t>(c)];
      if (reverseLookupValue >= 0x40) {
        // Skip invalid character
        continue;
      } else {
        filteredInputSize++;
      }
    }
  }

  // Update inputSize
  inputSize = inputSize - paddingSize;

  // Calculate extra bytes, if any
  auto extraBytes = filteredInputSize % kEncodedBlockByteSize;
  auto decodedSize =
      (filteredInputSize / kEncodedBlockByteSize) * kBinaryBlockByteSize;

  // Adjust the needed size for extra bytes, if present
  if (extraBytes) {
    if (extraBytes == 1) {
      VELOX_USER_FAIL(
          "Base64::decode() - invalid input string: "
          "string length cannot be 1 more than a multiple of 4.");
    }
    if (paddingSize != 0 && paddingSize + extraBytes < kEncodedBlockByteSize) {
      VELOX_USER_FAIL(
          "Base64::decode() - invalid input string: string length is not a multiple of 4.");
    }
    decodedSize += (extraBytes * kBinaryBlockByteSize) / kEncodedBlockByteSize;
  }

  return decodedSize;
}

// static
size_t Base64::decodeImpl(
    const char* input,
    size_t inputSize,
    char* outputBuffer,
    size_t outputSize,
    const ReverseIndex& reverseIndex) {
  if (!inputSize) {
    return 0;
  }

  auto decodedSize = calculateDecodedSize(input, inputSize, reverseIndex);
  if (outputSize < decodedSize) {
    VELOX_USER_FAIL(
        "Base64::decode() - invalid output string: "
        "output string is too small.");
  }

  while (true) {
    // Each character of the 4 encodes 6 bits of the original, grab each with
    // the appropriate shifts to rebuild the original and then split that back
    // into the original 8-bit bytes.
    uint32_t decodedBlock =
        (tryBase64ReverseLookup(input, reverseIndex) << 18) |
        (tryBase64ReverseLookup(input, reverseIndex) << 12);
    *outputBuffer++ = (decodedBlock >> 16) & 0xff;
    if (--decodedSize == 0) {
      break;
    }

    decodedBlock |= tryBase64ReverseLookup(input, reverseIndex) << 6;
    *outputBuffer++ = (decodedBlock >> 8) & 0xff;
    if (--decodedSize == 0) {
      break;
    }

    decodedBlock |= tryBase64ReverseLookup(input, reverseIndex);
    *outputBuffer++ = decodedBlock & 0xff;
    if (--decodedSize == 0) {
      break;
    }
  }

  return decodedSize;
}

// static
std::string Base64::encodeUrl(folly::StringPiece text, bool chunkBase64) {
  return encodeImpl(text, kBase64UrlCharset, false, chunkBase64);
}

// static
std::string Base64::encodeUrl(const char* input, size_t inputSize, bool chunkBase64) {
  return encodeUrl(folly::StringPiece(input, inputSize), chunkBase64);
}

// static
std::string Base64::encodeUrl(const folly::IOBuf* inputBuffer, bool chunkBase64) {
  return encodeImpl(IOBufWrapper(inputBuffer), kBase64UrlCharset, false, chunkBase64);
}

// static
void Base64::decodeUrl(
    const char* input,
    size_t inputSize,
    char* outputBuffer,
    size_t outputSize) {
  decodeImpl(input, inputSize, outputBuffer, outputSize, kBase64UrlReverseIndexTable);
}

// static
std::string Base64::decodeUrl(folly::StringPiece encodedText) {
  std::string decodedOutput;
  Base64::decodeUrl(
      std::make_pair(encodedText.data(), encodedText.size()), decodedOutput);
  return decodedOutput;
}

// static
void Base64::decodeUrl(
    const std::pair<const char*, int32_t>& payload,
    std::string& decodedOutput) {
  size_t decodedSize = (payload.second + 3) / 4 * 3;
  decodedOutput.resize(decodedSize, '\0');
  decodedSize = Base64::decodeImpl(
      payload.first,
      payload.second,
      &decodedOutput[0],
      decodedSize,
      kBase64UrlReverseIndexTable);
  decodedOutput.resize(decodedSize);
}

} // namespace facebook::velox::encoding
