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

package(default_visibility = ["//cc:__subpackages__"])

cc_library(
    name = "orchestrator_client",
    srcs = ["orchestrator_client.cc"],
    hdrs = ["orchestrator_client.h"],
    deps = [
        ":orchestrator_client_interface",
        "//cc/core/auth_token_client:auth_token_client_interface",
        "//cc/core/error:status_macros",
        "//cc/external:absl_bind_front",
        "//cc/external:absl_status",
        "//cc/external:absl_str_format",
        "//cc/external:absl_strings",
        "//cc/external:google_protobuf",
        "//cc/external:scp_core_async_context_lib",
        "//cc/external:scp_core_interface_lib",
        "//protos/core:cc_proto",
        "//protos/orchestrator/api:cc_proto",
    ],
)

cc_library(
    name = "orchestrator_client_interface",
    hdrs = ["orchestrator_client_interface.h"],
    deps = [
        "//cc/external:absl_status",
        "//cc/external:absl_str_format",
        "//cc/external:absl_strings",
        "//cc/external:google_protobuf",
        "//cc/external:scp_core_async_context_lib",
        "//cc/external:scp_core_interface_lib",
        "//protos/core:cc_proto",
        "//protos/orchestrator/api:cc_proto",
    ],
)

cc_test(
    name = "orchestrator_client_test",
    size = "small",
    srcs = ["orchestrator_client_test.cc"],
    deps = [
        ":orchestrator_client",
        "//cc/core/auth_token_client:auth_token_client_interface",
        "//cc/core/auth_token_client:mock_auth_token_client",
        "//cc/external:absl_status_matchers",
        "//cc/external:absl_str_format",
        "//cc/external:google_gtest_main",
        "//cc/external:scp_core_execution_result_matchers",
        "//cc/external:scp_core_http2_client_mock",
        "//cc/external:scp_core_interface_lib",
        "//cc/external:scp_core_test_utils",
        "//protos/core:cc_proto",
        "//protos/orchestrator/api:cc_proto",
    ],
)
