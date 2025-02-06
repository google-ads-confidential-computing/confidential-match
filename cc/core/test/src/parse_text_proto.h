/*
 * Copyright 2025 Google LLC
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

#ifndef CC_CORE_TEST_SRC_PARSE_TEXT_PROTO_H_
#define CC_CORE_TEST_SRC_PARSE_TEXT_PROTO_H_

#include <string>

#include "absl/strings/string_view.h"
#include "google/protobuf/message.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"

namespace google::confidential_match {

/**
 * @brief Parses a text proto or terminates the program. For use in tests only.
 */
class ParseTextProtoOrDie {
 public:
  explicit ParseTextProtoOrDie(absl::string_view text_proto)
      : text_proto_(text_proto) {}

  template <typename T>
  operator T() {
    T message;
    bool is_parse_successful =
        google::protobuf::TextFormat::ParseFromString(text_proto_, &message);
    if (!is_parse_successful) {
      ADD_FAILURE() << "Failed to parse the text proto into a message: "
                    << text_proto_;
      std::abort();
    }

    return message;
  }

 private:
  std::string text_proto_;
};

}  // namespace google::confidential_match

#endif  // CC_CORE_TEST_SRC_PARSE_TEXT_PROTO_H_
