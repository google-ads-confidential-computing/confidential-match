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
    name = "match_data_provider_test",
    size = "small",
    srcs = glob(["*.cc"]),
    copts = [
        "-std=c++17",
    ],
    deps = [
        "//cc/external:absl_strings",
        "//cc/external:absl_synchronization",
        "//cc/external:google_gtest_main",
        "//cc/external:scp_core_execution_result_matchers",
        "//cc/external:scp_core_interface_lib",
        "//cc/external:scp_core_test_utils",
        "//cc/external:scp_cpio_blob_storage_client_mock",
        "//cc/external:scp_cpio_crypto_client_mock",
        "//cc/external:tink_cc_mock_aead",
        "//cc/lookup_server/crypto_client/mock:crypto_client_mock",
        "//cc/lookup_server/match_data_provider/mock:match_data_provider_mock",
        "//cc/lookup_server/match_data_provider/src:match_data_provider",
        "//protos/lookup_server/backend:cc_proto",
    ],
)
