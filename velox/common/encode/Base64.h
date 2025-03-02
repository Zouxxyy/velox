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

#pragma once

#include <folly/Range.h>
#include <folly/io/IOBuf.h>

#include <array>
#include <string>

#include "velox/common/base/GTestMacros.h"

namespace facebook::velox::encoding {

class Base64 {
 public:
  static const size_t kCharsetSize = 64;
  static const size_t kReverseIndexSize = 256;

  /// Character set used for Base64 encoding.
  /// Contains specific characters that form the encoding scheme.
  using Charset = std::array<char, kCharsetSize>;

  /// Reverse lookup table for decoding.
  /// Maps each possible encoded character to its corresponding numeric value
  /// within the encoding base.
  using ReverseIndex = std::array<uint8_t, kReverseIndexSize>;

  // Reverse lookup tables for decoding
  static constexpr const Base64::ReverseIndex kBase64ReverseIndexTable = {
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 62,  255,
      255, 255, 63,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  255, 255,
      255, 255, 255, 255, 255, 0,   1,   2,   3,   4,   5,   6,   7,   8,   9,
      10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,
      25,  255, 255, 255, 255, 255, 255, 26,  27,  28,  29,  30,  31,  32,  33,
      34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,  48,
      49,  50,  51,  255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255};

  static constexpr const Base64::ReverseIndex kBase64UrlReverseIndexTable = {
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 62,  255,
      62,  255, 63,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  255, 255,
      255, 255, 255, 255, 255, 0,   1,   2,   3,   4,   5,   6,   7,   8,   9,
      10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,
      25,  255, 255, 255, 255, 63,  255, 26,  27,  28,  29,  30,  31,  32,  33,
      34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,  48,
      49,  50,  51,  255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255};

  /// Encodes the specified number of characters from the 'input'.
  static std::string
  encode(const char* input, size_t inputSize, bool chunkBase64 = true);

  /// Encodes the specified text.
  static std::string encode(folly::StringPiece text, bool chunkBase64 = true);

  /// Encodes the specified IOBuf data.
  static std::string encode(
      const folly::IOBuf* inputBuffer,
      bool chunkBase64 = true);

  /// Encodes the specified number of characters from the 'input' and writes the
  /// result to the 'outputBuffer'. The output must have enough space as
  /// returned by the calculateEncodedSize().
  static void encode(
      const char* input,
      size_t inputSize,
      char* outputBuffer,
      bool chunkBase64 = true);

  /// Encodes the specified number of characters from the 'input' using URL
  /// encoding.
  static std::string
  encodeUrl(const char* input, size_t inputSize, bool chunkBase64 = true);

  /// Encodes the specified text using URL encoding.
  static std::string encodeUrl(
      folly::StringPiece text,
      bool chunkBase64 = true);

  /// Encodes the specified IOBuf data using URL encoding.
  static std::string encodeUrl(
      const folly::IOBuf* inputBuffer,
      bool chunkBase64 = true);

  /// Encodes the specified number of characters from the 'input' and writes the
  /// result to the 'outputBuffer' using URL encoding. The output must have
  /// enough space as returned by the calculateEncodedSize().
  static void
  encodeUrl(const char* input, size_t inputSize, char* outputBuffer);

  /// Decodes the input Base64 encoded string.
  static std::string decode(folly::StringPiece encodedText);

  /// Decodes the specified encoded payload and writes the result to the
  /// 'output'.
  static void decode(
      const std::pair<const char*, int32_t>& payload,
      std::string& output);

  /// Decodes the specified number of characters from the 'input' and writes the
  /// result to the 'outputBuffer'. The output must have enough space as
  /// returned by the calculateDecodedSize().
  static void decode(const char* input, size_t inputSize, char* outputBuffer);

  /// Decodes the specified number of characters from the 'input' and writes the
  /// result to the 'outputBuffer'.
  static size_t decode(
      const char* input,
      size_t inputSize,
      char* outputBuffer,
      size_t outputSize);

  /// Decodes the input Base64 URL encoded string.
  static std::string decodeUrl(folly::StringPiece encodedText);

  /// Decodes the specified URL encoded payload and writes the result to the
  /// 'output'.
  static void decodeUrl(
      const std::pair<const char*, int32_t>& payload,
      std::string& output);

  /// Decodes the specified number of characters from the 'input' using URL
  /// encoding and writes the result to the 'outputBuffer'
  static void decodeUrl(
      const char* input,
      size_t inputSize,
      char* outputBuffer,
      size_t outputSize);

  /// Calculates the encoded size based on input 'inputSize'.
  static size_t calculateEncodedSize(
      size_t inputSize,
      bool withPadding = true,
      bool chunkBase64 = true);

  /// Returns the actual size of the decoded data. Removes the padding
  /// length from the input data 'inputSize'.
  static size_t calculateDecodedSize(
      const char* input,
      size_t& inputSize,
      const ReverseIndex& reverseIndex = kBase64ReverseIndexTable);

 private:
  // Padding character used in encoding.
  static const char kPadding = '=';

  // Checks if the input Base64 string is padded.
  static inline bool isPadded(const char* input, size_t inputSize) {
    return (inputSize > 0 && input[inputSize - 1] == kPadding);
  }

  // Counts the number of padding characters in encoded input.
  static inline size_t numPadding(const char* input, size_t inputSize) {
    size_t numPadding{0};
    while (inputSize > 0 && input[inputSize - 1] == kPadding) {
      numPadding++;
      inputSize--;
    }
    return numPadding;
  }

  // Reverse lookup helper function to get the original index of a Base64
  // character.
  static uint8_t base64ReverseLookup(
      char encodedChar,
      const ReverseIndex& reverseIndex);

  static uint8_t tryBase64ReverseLookup(
      const char*& input,
      const ReverseIndex& reverseIndex);

  // Encodes the specified data using the provided charset.
  template <class T>
  static std::string encodeImpl(
      const T& input,
      const Charset& charset,
      bool includePadding,
      bool chunkBase64);

  // Encodes the specified data using the provided charset.
  template <class T>
  static void encodeImpl(
      const T& input,
      const Charset& charset,
      bool includePadding,
      char* outputBuffer,
      bool chunkBase64);

  // Decodes the specified data using the provided reverse lookup table.
  static size_t decodeImpl(
      const char* input,
      size_t inputSize,
      char* outputBuffer,
      size_t outputSize,
      const ReverseIndex& reverseIndex);

  VELOX_FRIEND_TEST(Base64Test, checksPadding);
  VELOX_FRIEND_TEST(Base64Test, countsPaddingCorrectly);
};

} // namespace facebook::velox::encoding
