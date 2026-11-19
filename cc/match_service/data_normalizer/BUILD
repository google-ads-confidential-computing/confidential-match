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
    name = "user_data_normalizer",
    srcs = ["user_data_normalizer.cc"],
    hdrs = ["user_data_normalizer.h"],
    deps = [
        "//cc/core/error:status_macros",
        "//cc/core/hash:hasher_interface",
        "//cc/external:scp_core_execution_result",
        "//cc/external:scp_cpio_plaintext_pii",
        "//cc/match_service/error",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "user_data_normalizer_test",
    size = "small",
    srcs = ["user_data_normalizer_test.cc"],
    deps = [
        ":user_data_normalizer",
        "//cc/core/hash:mock_hasher",
        "//cc/core/hash:sha256_hasher",
        "//cc/external:absl_status_matchers",
        "//cc/match_service/error",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
    ],
)
