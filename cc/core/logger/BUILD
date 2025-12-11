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

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "logger",
    srcs = ["logger.cc"],
    hdrs = [
        "log.h",
        "logger.h",
        "logger_interface.h",
    ],
    deps = [
        "//cc/external:absl_base_core_headers",
        "//cc/external:absl_log",
        "//cc/external:absl_strings",
    ],
)

cc_test(
    name = "logger_test",
    srcs = ["logger_test.cc"],
    deps = [
        ":logger",
        "//cc/external:absl_log_scoped_mock_log",
        "//cc/external:google_gtest",
        "//cc/external:google_gtest_main",
    ],
)

cc_test(
    name = "log_test",
    srcs = ["log_test.cc"],
    deps = [
        ":logger",
        "//cc/external:absl_log_scoped_mock_log",
        "//cc/external:google_gtest",
        "//cc/external:google_gtest_main",
    ],
)
