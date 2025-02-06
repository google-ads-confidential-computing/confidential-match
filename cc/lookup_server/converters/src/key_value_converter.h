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

#ifndef CC_LOOKUP_SERVER_CONVERTERS_SRC_KEY_VALUE_CONVERTER_H_
#define CC_LOOKUP_SERVER_CONVERTERS_SRC_KEY_VALUE_CONVERTER_H_

#include "cc/public/core/interface/execution_result.h"

#include "protos/core/data_value.pb.h"
#include "protos/lookup_server/api/lookup.pb.h"
#include "protos/lookup_server/backend/exporter_data_row.pb.h"

namespace google::confidential_match::lookup_server {

/**
 * @brief Converts a DataValue key-value pair to an ExporterAssociatedData
 * key-value pair.
 *
 * @param kv the DataValue key-value pair
 * @param out the ExporterAssociatedData key-value pair to be written to
 * @return an ExecutionResult indicating whether the operation was successful
 */
scp::core::ExecutionResult ConvertKeyValue(
    const ::google::confidential_match::KeyValue& kv,
    proto_backend::ExporterAssociatedData_KeyValue& out);

/**
 * @brief Converts an ExporterAssociatedData key-value pair to a DataValue
 * key-value pair.
 *
 * @param kv the ExporterAssociatedData key-value pair
 * @param out the DataValue key-value pair to be written to
 * @return an ExecutionResult indicating whether the operation was successful
 */
scp::core::ExecutionResult ConvertKeyValue(
    const proto_backend::ExporterAssociatedData_KeyValue& kv,
    ::google::confidential_match::KeyValue& out);

/**
 * @brief Converts an API key-value pair to a core backend key-value pair.
 *
 * @param kv the API key-value pair
 * @param out the core backend key-value pair to be written to
 * @return an ExecutionResult indicating whether the operation was successful
 */
scp::core::ExecutionResult ConvertKeyValue(
    const proto_api::KeyValue& kv, ::google::confidential_match::KeyValue& out);

/**
 * @brief Converts a core backend key-value pair to an API key-value pair.
 *
 * @param kv the core backend key-value pair
 * @param out the API key-value pair to be written to
 * @return an ExecutionResult indicating whether the operation was successful
 */
scp::core::ExecutionResult ConvertKeyValue(
    const ::google::confidential_match::KeyValue& kv, proto_api::KeyValue& out);

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_CONVERTERS_SRC_KEY_VALUE_CONVERTER_H_
