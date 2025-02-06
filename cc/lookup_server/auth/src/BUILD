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

package(default_visibility = ["//cc/lookup_server:__subpackages__"])

cc_library(
    name = "auth",
    srcs = glob(
        [
            "*.h",
            "*.cc",
        ],
    ),
    copts = [
        "-std=c++17",
    ],
    deps = [
        "//cc/external:absl_flat_hash_set",
        "//cc/external:absl_strings",
        "//cc/external:absl_time",
        "//cc/external:scp_core_common_uuid",
        "//cc/external:scp_core_errors_lib",
        "//cc/external:scp_core_interface_lib",
        "//cc/external:scp_core_logger",
        "//cc/external:tink_cc",
        "//cc/external:tink_jwt_jwk_set_converter",
        "//cc/external:tink_jwt_public_key_verify",
        "//cc/external:tink_jwt_signature_config",
        "//cc/external:tink_jwt_validator",
        "//cc/external:tink_jwt_verified_jwt",
        "//cc/lookup_server/interface:lookup_server_interface_lib",
    ],
)
