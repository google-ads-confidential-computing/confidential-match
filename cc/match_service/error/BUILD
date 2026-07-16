# Copyright 2025 Google LLC
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
    name = "error",
    srcs = ["error.cc"],
    hdrs = ["error.h"],
    deps = [
        "//cc/core/logger",
        "//cc/external:absl_status",
        "//cc/external:absl_strings",
        "//cc/external:google_rpc_code_cc_proto",
        "//cc/external:google_rpc_error_details_cc_proto",
        "//cc/external:google_rpc_status_cc_proto",
        "//cc/external:grpc++",
        "//protos/match_service/api/v1:error_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
    ],
)

cc_test(
    name = "error_test",
    srcs = ["error_test.cc"],
    deps = [
        ":error",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//cc/external:google_rpc_error_details_cc_proto",
        "//cc/external:google_rpc_status_cc_proto",
        "//cc/external:scp_core_test_utils",
        "//protos/match_service/api/v1:error_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
    ],
)
