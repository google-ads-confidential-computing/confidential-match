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

package(default_visibility = ["//cc/lookup_server:__subpackages__"])

cc_test(
    name = "match_data_loader_test",
    size = "small",
    srcs = ["match_data_loader_test.cc"],
    copts = [
        "-std=c++17",
    ],
    deps = [
        "//cc/external:absl_span",
        "//cc/external:absl_strings",
        "//cc/external:google_gtest_main",
        "//cc/external:scp_core_execution_result_matchers",
        "//cc/external:scp_core_interface_lib",
        "//cc/external:scp_core_test_utils",
        "//cc/lookup_server/crypto_client/mock:crypto_client_mock",
        "//cc/lookup_server/interface:lookup_server_interface_lib",
        "//cc/lookup_server/match_data_loader/mock:match_data_loader_mock",
        "//cc/lookup_server/match_data_loader/src:match_data_loader",
        "//cc/lookup_server/match_data_provider/mock:match_data_provider_mock",
        "//cc/lookup_server/match_data_storage/mock:match_data_storage_mock",
        "//cc/lookup_server/metric_client/mock:metric_client_mock",
        "//cc/lookup_server/orchestrator_client/mock:orchestrator_client_mock",
        "//cc/lookup_server/types:lookup_server_types_lib",
        "//protos/lookup_server/backend:cc_proto",
    ],
)
