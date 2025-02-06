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
    name = "parsers_lib",
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
        "//cc/external:absl_base_core_headers",
        "//cc/external:absl_str_format",
        "//cc/external:absl_strings",
        "//cc/external:absl_synchronization",
        "//cc/external:google_protobuf",
        "//cc/external:scp_core_common_uuid",
        "//cc/external:scp_core_concurrent_queue",
        "//cc/external:scp_core_errors_lib",
        "//cc/external:scp_core_interface_lib",
        "//cc/external:scp_core_logger",
        "//cc/lookup_server/converters/src:converters_lib",
        "//cc/lookup_server/crypto_client/src:crypto_client",
        "//cc/lookup_server/interface:lookup_server_interface_lib",
        "//cc/lookup_server/types:lookup_server_types_lib",
        "//protos/lookup_server/backend:cc_proto",
    ],
)
