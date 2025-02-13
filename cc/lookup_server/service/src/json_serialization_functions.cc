// Copyright 2025 Google LLC
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

#include "cc/lookup_server/service/src/json_serialization_functions.h"

#include <memory>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "cc/core/interface/type_def.h"
#include "google/protobuf/message.h"
#include "google/protobuf/util/json_util.h"

namespace google::confidential_match::lookup_server {

using ::google::protobuf::Message;
using google::protobuf::util::JsonStringToMessage;
using google::protobuf::util::MessageToJsonString;
using ::google::scp::core::BytesBuffer;

absl::Status JsonBytesBufferToProto(const BytesBuffer& bytes_buffer,
                                    Message* out) {
  absl::string_view input(bytes_buffer.bytes->data(), bytes_buffer.length);
  if (!JsonStringToMessage(input, out).ok()) {
    return absl::InvalidArgumentError("Failed to parse bytes buffer to proto.");
  }
  return absl::OkStatus();
}

absl::Status ProtoToJsonBytesBuffer(const Message& message, BytesBuffer* out) {
  std::string message_str;
  if (!MessageToJsonString(message, &message_str).ok()) {
    return absl::InternalError("Failed to convert proto to JSON string.");
  }
  out->capacity = message_str.length();
  out->length = message_str.length();
  out->bytes = std::make_shared<std::vector<scp::core::Byte>>(
      message_str.begin(), message_str.end());
  return absl::OkStatus();
}

}  // namespace google::confidential_match::lookup_server
