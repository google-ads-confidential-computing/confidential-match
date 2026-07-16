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

load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_cc//cc:cc_test.bzl", "cc_test")

package(default_visibility = ["//cc:__subpackages__"])

cc_library(
    name = "hasher_interface",
    hdrs = ["hasher_interface.h"],
    deps = [
        "//cc/external:absl_statusor",
        "//cc/external:absl_strings",
    ],
)

cc_library(
    name = "mock_hasher",
    testonly = True,
    hdrs = ["mock_hasher.h"],
    deps = [
        ":hasher_interface",
        "//cc/external:absl_statusor",
        "//cc/external:absl_strings",
        "//cc/external:google_gtest",
    ],
)

cc_library(
    name = "sha256_hasher",
    srcs = ["sha256_hasher.cc"],
    hdrs = ["sha256_hasher.h"],
    deps = [
        ":hasher_interface",
        "//cc/external:absl_status",
        "//cc/external:absl_statusor",
        "//cc/external:absl_strings",
        "//cc/external:boringssl_crypto",
    ],
)

cc_test(
    name = "sha256_hasher_test",
    srcs = ["sha256_hasher_test.cc"],
    deps = [
        ":mock_hasher",
        ":sha256_hasher",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
    ],
)
