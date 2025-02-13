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

cc_test(
    name = "lookup_server_auth_gcp_test",
    srcs = glob(["*.cc"]),
    deps = [
        "//cc/external:absl_status",
        "//cc/external:absl_strings",
        "//cc/external:google_gtest_main",
        "//cc/external:google_protobuf",
        "//cc/external:scp_core_async_context_lib",
        "//cc/external:scp_core_common_serialization",
        "//cc/external:scp_core_config_provider_mock",
        "//cc/external:scp_core_execution_result_matchers",
        "//cc/external:scp_core_http2_client",
        "//cc/external:scp_core_interface_lib",
        "//cc/lookup_server/auth/mock:auth_mock",
        "//cc/lookup_server/auth/src:auth",
        "//cc/lookup_server/auth/src/gcp:lookup_server_auth_gcp_lib",
    ],
)
