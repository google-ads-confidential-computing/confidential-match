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

#include "cc/match_service/tasks/associated_data_collector.h"

#include <vector>

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "gmock/gmock.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"

#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::absl::StatusCode;
using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using ::google::confidential_match::match_service::backend::AssociatedData;
using ::google::confidential_match::match_service::backend::AssociatedDataType;
using ::google::confidential_match::match_service::backend::KeyValue;
using ::google::protobuf::TextFormat;
using ::google::scp::core::test::EqualsProto;
using ::testing::ElementsAre;
using ::testing::IsEmpty;

TEST(AssociatedDataCollectorTest, AddSuccess) {
  AssociatedDataCollector collector;
  AssociatedData associated_data;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        first_party_identifier { id: "example" }
      )pb",
      &associated_data));

  int index = collector.Add(associated_data);

  EXPECT_EQ(index, 0);
}

TEST(AssociatedDataCollectorTest, GetSingleSuccess) {
  AssociatedDataCollector collector;
  AssociatedData associated_data;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        first_party_identifier { id: "example" }
      )pb",
      &associated_data));

  collector.Add(associated_data);
  std::vector<AssociatedData> data_list = collector.Get();

  EXPECT_THAT(data_list, ElementsAre(EqualsProto(associated_data)));
}

TEST(AssociatedDataCollectorTest, AddDifferentAssociatedData) {
  AssociatedDataCollector collector;
  AssociatedData associated_data;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        first_party_identifier { id: "example" }
      )pb",
      &associated_data));
  AssociatedData associated_data2;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        first_party_identifier { id: "example2" }
      )pb",
      &associated_data2));

  int index = collector.Add(associated_data);
  int index2 = collector.Add(associated_data2);
  std::vector<AssociatedData> data_list = collector.Get();

  EXPECT_EQ(index, 0);
  EXPECT_EQ(index2, 1);
  EXPECT_THAT(data_list, ElementsAre(EqualsProto(associated_data),
                                     EqualsProto(associated_data2)));
}

TEST(AssociatedDataCollectorTest, AddDuplicateAssociatedData) {
  AssociatedDataCollector collector;
  AssociatedData associated_data;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        first_party_identifier { id: "example" }
      )pb",
      &associated_data));
  AssociatedData associated_data2;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        first_party_identifier { id: "example2" }
      )pb",
      &associated_data2));
  AssociatedData associated_data3(associated_data);

  int index = collector.Add(associated_data);
  int index2 = collector.Add(associated_data2);
  int index3 = collector.Add(associated_data3);
  std::vector<AssociatedData> data_list = collector.Get();

  EXPECT_EQ(index, 0);
  EXPECT_EQ(index2, 1);
  EXPECT_EQ(index3, 0);
  EXPECT_THAT(data_list, ElementsAre(EqualsProto(associated_data),
                                     EqualsProto(associated_data2)));
}

TEST(AssociatedDataCollectorTest, GetEmpty) {
  AssociatedDataCollector collector;
  EXPECT_THAT(collector.Get(), IsEmpty());
}

}  // namespace
}  // namespace google::confidential_match::match_service
