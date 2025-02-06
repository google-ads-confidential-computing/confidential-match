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

#ifndef CC_LOOKUP_SERVER_SERVICE_SRC_JSON_SERIALIZATION_FUNCTIONS_H_
#define CC_LOOKUP_SERVER_SERVICE_SRC_JSON_SERIALIZATION_FUNCTIONS_H_

#include "absl/status/status.h"
#include "cc/core/interface/type_def.h"
#include "google/protobuf/message.h"
#include "src/google/protobuf/message.h"

namespace google::confidential_match::lookup_server {

// Converts data from a BytesBuffer object into a proto, returning whether or
// not the operation was successful.
absl::Status JsonBytesBufferToProto(
    const google::scp::core::BytesBuffer& bytes_buffer,
    google::protobuf::Message* out);

// Converts a proto to a serialized JSON string within a BytesBuffer.
// Returns a status indicating whether or not the operation was successful.
absl::Status ProtoToJsonBytesBuffer(const google::protobuf::Message& message,
                                    google::scp::core::BytesBuffer* out);

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_SERVICE_SRC_JSON_SERIALIZATION_FUNCTIONS_H_
