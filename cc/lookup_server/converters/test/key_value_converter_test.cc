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

#include "cc/lookup_server/converters/src/key_value_converter.h"

#include "include/gtest/gtest.h"
#include "public/core/interface/execution_result.h"

#include "protos/core/data_value.pb.h"
#include "protos/lookup_server/backend/exporter_data_row.pb.h"

namespace google::confidential_match::lookup_server {

using ::google::confidential_match::lookup_server::proto_backend::
    ExporterAssociatedData_KeyValue;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
namespace proto_core = ::google::confidential_match;

TEST(KeyValueConverterTest, ConvertKeyValueCoreToExporterConvertsEmptyObject) {
  proto_core::KeyValue kv;
  ExporterAssociatedData_KeyValue out;

  ExecutionResult result = ConvertKeyValue(kv, out);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(out.key(), "");
  EXPECT_EQ(out.value_case(), ExporterAssociatedData_KeyValue::VALUE_NOT_SET);
}

TEST(KeyValueConverterTest, ConvertKeyValueCoreToExporterConvertsKey) {
  proto_core::KeyValue kv;
  *kv.mutable_key() = "sample_key";
  ExporterAssociatedData_KeyValue out;

  ExecutionResult result = ConvertKeyValue(kv, out);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(out.key(), "sample_key");
}

TEST(KeyValueConverterTest, ConvertKeyValueCoreToExporterConvertsStringValue) {
  proto_core::KeyValue kv;
  *kv.mutable_string_value() = "string";
  ExporterAssociatedData_KeyValue out;

  ExecutionResult result = ConvertKeyValue(kv, out);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(out.string_value(), "string");
}

TEST(KeyValueConverterTest, ConvertKeyValueCoreToExporterConvertsIntValue) {
  proto_core::KeyValue kv;
  kv.set_int_value(1);
  ExporterAssociatedData_KeyValue out;

  ExecutionResult result = ConvertKeyValue(kv, out);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(out.int_value(), 1);
}

TEST(KeyValueConverterTest, ConvertKeyValueCoreToExporterConvertsDoubleValue) {
  proto_core::KeyValue kv;
  kv.set_double_value(1.0);
  ExporterAssociatedData_KeyValue out;

  ExecutionResult result = ConvertKeyValue(kv, out);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(out.double_value(), 1.0);
}

TEST(KeyValueConverterTest, ConvertKeyValueCoreToExporterConvertsBoolValue) {
  proto_core::KeyValue kv;
  kv.set_bool_value(true);
  ExporterAssociatedData_KeyValue out;

  ExecutionResult result = ConvertKeyValue(kv, out);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_TRUE(out.bool_value());
}

TEST(KeyValueConverterTest, ConvertKeyValueCoreToExporterConvertsBytesValue) {
  proto_core::KeyValue kv;
  *kv.mutable_bytes_value() = "bytes";
  ExporterAssociatedData_KeyValue out;

  ExecutionResult result = ConvertKeyValue(kv, out);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(out.bytes_value(), "bytes");
}

TEST(KeyValueConverterTest, ConvertKeyValueExporterToCoreConvertsEmptyObject) {
  ExporterAssociatedData_KeyValue kv;
  proto_core::KeyValue out;

  ExecutionResult result = ConvertKeyValue(kv, out);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(out.key(), "");
  EXPECT_EQ(out.value_case(), proto_core::KeyValue::VALUE_NOT_SET);
}

TEST(KeyValueConverterTest, ConvertKeyValueExporterToCoreConvertsKey) {
  ExporterAssociatedData_KeyValue kv;
  *kv.mutable_key() = "sample_key";
  proto_core::KeyValue out;

  ExecutionResult result = ConvertKeyValue(kv, out);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(out.key(), "sample_key");
}

TEST(KeyValueConverterTest, ConvertKeyValueExporterToCoreConvertsStringValue) {
  ExporterAssociatedData_KeyValue kv;
  *kv.mutable_string_value() = "string";
  proto_core::KeyValue out;

  ExecutionResult result = ConvertKeyValue(kv, out);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(out.string_value(), "string");
}

TEST(KeyValueConverterTest, ConvertKeyValueExporterToCoreConvertsIntValue) {
  ExporterAssociatedData_KeyValue kv;
  kv.set_int_value(1);
  proto_core::KeyValue out;

  ExecutionResult result = ConvertKeyValue(kv, out);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(out.int_value(), 1);
}

TEST(KeyValueConverterTest, ConvertKeyValueExporterToCoreConvertsDoubleValue) {
  ExporterAssociatedData_KeyValue kv;
  kv.set_double_value(1.0);
  proto_core::KeyValue out;

  ExecutionResult result = ConvertKeyValue(kv, out);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(out.double_value(), 1.0);
}

TEST(KeyValueConverterTest, ConvertKeyValueExporterToCoreConvertsBoolValue) {
  ExporterAssociatedData_KeyValue kv;
  kv.set_bool_value(true);
  proto_core::KeyValue out;

  ExecutionResult result = ConvertKeyValue(kv, out);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_TRUE(out.bool_value());
}

TEST(KeyValueConverterTest, ConvertKeyValueExporterToCoreConvertsBytesValue) {
  ExporterAssociatedData_KeyValue kv;
  *kv.mutable_bytes_value() = "bytes";
  proto_core::KeyValue out;

  ExecutionResult result = ConvertKeyValue(kv, out);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(out.bytes_value(), "bytes");
}

TEST(KeyValueConverterTest, ConvertKeyValueApiToCoreConvertsEmptyObject) {
  proto_api::KeyValue kv;
  proto_core::KeyValue out;

  ExecutionResult result = ConvertKeyValue(kv, out);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(out.key(), "");
  EXPECT_EQ(out.value_case(), proto_core::KeyValue::VALUE_NOT_SET);
}

TEST(KeyValueConverterTest, ConvertKeyValueApiToCoreConvertsKey) {
  proto_api::KeyValue kv;
  *kv.mutable_key() = "sample_key";
  proto_core::KeyValue out;

  ExecutionResult result = ConvertKeyValue(kv, out);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(out.key(), "sample_key");
}

TEST(KeyValueConverterTest, ConvertKeyValueApiToCoreConvertsStringValue) {
  proto_api::KeyValue kv;
  *kv.mutable_string_value() = "string";
  proto_core::KeyValue out;

  ExecutionResult result = ConvertKeyValue(kv, out);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(out.string_value(), "string");
}

TEST(KeyValueConverterTest, ConvertKeyValueApiToCoreConvertsIntValue) {
  proto_api::KeyValue kv;
  kv.set_int_value(1);
  proto_core::KeyValue out;

  ExecutionResult result = ConvertKeyValue(kv, out);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(out.int_value(), 1);
}

TEST(KeyValueConverterTest, ConvertKeyValueApiToCoreConvertsDoubleValue) {
  proto_api::KeyValue kv;
  kv.set_double_value(1.0);
  proto_core::KeyValue out;

  ExecutionResult result = ConvertKeyValue(kv, out);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(out.double_value(), 1.0);
}

TEST(KeyValueConverterTest, ConvertKeyValueApiToCoreConvertsBoolValue) {
  proto_api::KeyValue kv;
  kv.set_bool_value(true);
  proto_core::KeyValue out;

  ExecutionResult result = ConvertKeyValue(kv, out);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_TRUE(out.bool_value());
}

TEST(KeyValueConverterTest, ConvertKeyValueApiToCoreConvertsBytesValue) {
  proto_api::KeyValue kv;
  *kv.mutable_bytes_value() = "bytes";
  proto_core::KeyValue out;

  ExecutionResult result = ConvertKeyValue(kv, out);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(out.bytes_value(), "bytes");
}

TEST(KeyValueConverterTest, ConvertKeyValueCoreToApiConvertsEmptyObject) {
  proto_core::KeyValue kv;
  proto_api::KeyValue out;

  ExecutionResult result = ConvertKeyValue(kv, out);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(out.key(), "");
  EXPECT_EQ(out.value_case(), proto_api::KeyValue::VALUE_NOT_SET);
}

TEST(KeyValueConverterTest, ConvertKeyValueCoreToApiConvertsKey) {
  proto_core::KeyValue kv;
  *kv.mutable_key() = "sample_key";
  proto_api::KeyValue out;

  ExecutionResult result = ConvertKeyValue(kv, out);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(out.key(), "sample_key");
}

TEST(KeyValueConverterTest, ConvertKeyValueCoreToApiConvertsStringValue) {
  proto_core::KeyValue kv;
  *kv.mutable_string_value() = "string";
  proto_api::KeyValue out;

  ExecutionResult result = ConvertKeyValue(kv, out);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(out.string_value(), "string");
}

TEST(KeyValueConverterTest, ConvertKeyValueCoreToApiConvertsIntValue) {
  proto_core::KeyValue kv;
  kv.set_int_value(1);
  proto_api::KeyValue out;

  ExecutionResult result = ConvertKeyValue(kv, out);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(out.int_value(), 1);
}

TEST(KeyValueConverterTest, ConvertKeyValueCoreToApiConvertsDoubleValue) {
  proto_core::KeyValue kv;
  kv.set_double_value(1.0);
  proto_api::KeyValue out;

  ExecutionResult result = ConvertKeyValue(kv, out);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(out.double_value(), 1.0);
}

TEST(KeyValueConverterTest, ConvertKeyValueCoreToApiConvertsBoolValue) {
  proto_core::KeyValue kv;
  kv.set_bool_value(true);
  proto_api::KeyValue out;

  ExecutionResult result = ConvertKeyValue(kv, out);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_TRUE(out.bool_value());
}

TEST(KeyValueConverterTest, ConvertKeyValueCoreToApiConvertsBytesValue) {
  proto_core::KeyValue kv;
  *kv.mutable_bytes_value() = "bytes";
  proto_api::KeyValue out;

  ExecutionResult result = ConvertKeyValue(kv, out);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(out.bytes_value(), "bytes");
}

}  // namespace google::confidential_match::lookup_server
