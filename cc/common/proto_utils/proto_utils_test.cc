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

#include "cc/common/proto_utils/proto_utils.h"

#include <gtest/gtest.h>

#include <string>

#include "protos/lookup_server/api/healthcheck.pb.h"

namespace google::confidential_match::common {
namespace {

using ::google::confidential_match::lookup_server::proto_api::Service;

TEST(ProtoUtilsTest, PrintsProtoCorrectly) {
  Service service;
  service.set_name("test_service");
  service.set_status("OK");

  std::string result = ProtoUtils::TextProtoString(service);
  EXPECT_EQ(result, "name: \"test_service\"\nstatus: \"OK\"\n");
}

TEST(ProtoUtilsTest, PrintsProtoShortCorrectly) {
  Service service;
  service.set_name("test_service");
  service.set_status("OK");

  std::string result = ProtoUtils::ShortTextProtoString(service);
  EXPECT_EQ(result, "name: \"test_service\" status: \"OK\" ");
}

}  // namespace
}  // namespace google::confidential_match::common
