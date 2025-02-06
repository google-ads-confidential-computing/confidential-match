# Copyright 2024 Google LLC
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

load("@rules_cc//cc:defs.bzl", "cc_test")

package(default_visibility = ["//cc/lookup_server:__subpackages__"])

cc_test(
    name = "health_service_test",
    size = "small",
    srcs = ["health_service_test.cc"],
    copts = [
        "-std=c++17",
    ],
    linkopts = [
        "-latomic",
    ],
    deps = [
        "//cc/external:google_gtest_main",
        "//cc/external:scp_core_execution_result_matchers",
        "//cc/external:scp_core_http2_server_mock",
        "//cc/external:scp_core_interface_lib",
        "//cc/lookup_server/health_service/mock:health_service_mock",
        "//cc/lookup_server/health_service/src:health_service_lib",
        "//cc/lookup_server/interface:lookup_server_interface_lib",
        "//cc/lookup_server/match_data_storage/mock:match_data_storage_mock",
        "//cc/lookup_server/service/src:lookup_service_lib",
    ],
)
