# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

load("@rules_cc//cc:defs.bzl", "cc_library", "cc_test")

package(default_visibility = ["//cc/match_service:__subpackages__"])

cc_library(
    name = "match_key_encoding_validator",
    srcs = ["match_key_encoding_validator.cc"],
    hdrs = ["match_key_encoding_validator.h"],
    deps = [
        "//cc/external:absl_status",
        "//cc/external:absl_strings",
        "//cc/match_service/error",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "match_key_encoding_validator_test",
    size = "small",
    srcs = ["match_key_encoding_validator_test.cc"],
    deps = [
        ":match_key_encoding_validator",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)
