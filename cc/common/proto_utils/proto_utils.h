// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef CC_COMMON_PROTO_UTILS_PROTO_UTILS_H_
#define CC_COMMON_PROTO_UTILS_PROTO_UTILS_H_

#include <string>

#include "google/protobuf/message.h"

namespace google::confidential_match::common {

class ProtoUtils {
 public:
  /**
   * @brief Helper to format proto to clean text-format string without
   * DebugString anti-parsing commentary. See:
   * https://protobuf.dev/programming-guides/deserialize-debug/
   */
  static std::string TextProtoString(const google::protobuf::Message& message);

  /**
   * @brief Helper to format proto to clean text-format string without
   * ShortDebugString anti-parsing commentary and on one line. See:
   * https://protobuf.dev/programming-guides/deserialize-debug/
   */
  static std::string ShortTextProtoString(
      const google::protobuf::Message& message);
};

}  // namespace google::confidential_match::common

#endif  // CC_COMMON_PROTO_UTILS_PROTO_UTILS_H_
