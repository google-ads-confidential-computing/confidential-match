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

#include "cc/lookup_server/parsers/src/export_metadata_parser.h"

#include <string>

#include "cc/public/core/interface/execution_result.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "gtest/gtest.h"

#include "cc/lookup_server/parsers/src/error_codes.h"
#include "protos/lookup_server/backend/export_metadata.pb.h"

namespace google::confidential_match::lookup_server {

using ::google::confidential_match::lookup_server::proto_backend::
    ExportMetadata;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::test::IsSuccessful;
using ::google::scp::core::test::ResultIs;

TEST(ExportMetadataParserTest, ParseExportMetadataParsesEncryptedDek) {
  std::string data = R"({"encrypted_dek":"dek"})";

  ExecutionResultOr<ExportMetadata> export_metadata_or =
      ParseExportMetadata(data);

  EXPECT_THAT(export_metadata_or.result(), IsSuccessful());
  EXPECT_EQ(export_metadata_or->encrypted_dek(), "dek");
}

TEST(ExportMetadataParserTest, ParseExportMetadataEmptyDataReturnsFailure) {
  ExecutionResultOr<ExportMetadata> export_metadata_or =
      ParseExportMetadata("");
  EXPECT_THAT(export_metadata_or.result(),
              ResultIs(FailureExecutionResult(PARSER_INVALID_EXPORT_METADATA)));
}

TEST(ExportMetadataParserTest, ParseExportMetadataInvalidDataReturnsFailure) {
  std::string data = R"({"invalid-key":"invalid"})";

  ExecutionResultOr<ExportMetadata> export_metadata_or =
      ParseExportMetadata(data);

  EXPECT_THAT(export_metadata_or.result(),
              ResultIs(FailureExecutionResult(PARSER_INVALID_EXPORT_METADATA)));
}

}  // namespace google::confidential_match::lookup_server
