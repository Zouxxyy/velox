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

#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace facebook::velox::functions::sparksql::test {
namespace {
class Base64Test : public SparkFunctionBaseTest {
 protected:
 protected:
  template <typename T>
  std::optional<std::string> base64(std::optional<T> v) {
    return evaluateOnce<std::string>("base64(cast(c0 as varbinary))", v);
  }

  template <typename T>
  std::optional<std::string> base64(
      std::optional<T> v,
      std::optional<bool> chunkBase64) {
    return evaluateOnce<std::string>(
        "base64(cast(c0 as varbinary), c1)", v, chunkBase64);
  }
};

TEST_F(Base64Test, chunkBase64) {
  std::string s(58, 'a');
  EXPECT_EQ(
      "YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFh\r\nYQ==",
      base64<std::string>(s));
  EXPECT_EQ(
      "YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFh\r\nYQ==",
      base64<std::string>(s, true));
  EXPECT_EQ(
      "YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYQ==",
      base64<std::string>(s, false));
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
