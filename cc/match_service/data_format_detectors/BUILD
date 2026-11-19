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
    name = "address_format_detector",
    srcs = ["address_format_detector.cc"],
    hdrs = [
        "address_format_detector.h",
        "canonical_countries.h",
    ],
    deps = [
        "//cc/match_service/error",
        "//cc/match_service/tasks:normalization_utils",
        "//cc/match_service/validators:match_key_encoding_validator",
        "//protos/match_service/backend:match_service_cc_proto",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "address_format_detector_test",
    size = "small",
    srcs = ["address_format_detector_test.cc"],
    deps = [
        ":address_format_detector",
        "//cc/match_service/error",
        "//protos/match_service/backend:match_service_cc_proto",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "user_data_detector",
    srcs = ["user_data_detector.cc"],
    hdrs = ["user_data_detector.h"],
    deps = [
        "//cc/external:scp_cpio_format_detection",
        "//cc/match_service/error",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "user_data_detector_test",
    size = "small",
    srcs = ["user_data_detector_test.cc"],
    deps = [
        ":user_data_detector",
        "//cc/external:absl_status_matchers",
        "//cc/external:scp_cpio_format_detection",
        "//cc/match_service/error",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
        "@com_google_googletest//:gtest_main",
    ],
)
