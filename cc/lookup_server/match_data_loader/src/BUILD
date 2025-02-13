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

cc_library(
    name = "match_data_loader",
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
        "//cc/core/data_table/src:data_table_lib",
        "//cc/external:absl_flat_hash_map",
        "//cc/external:absl_str_format",
        "//cc/external:absl_strings",
        "//cc/external:absl_synchronization",
        "//cc/external:absl_time",
        "//cc/external:google_protobuf",
        "//cc/external:scp_core_common_uuid",
        "//cc/external:scp_core_concurrent_queue",
        "//cc/external:scp_core_errors_lib",
        "//cc/external:scp_core_interface_lib",
        "//cc/external:scp_core_logger",
        "//cc/external:scp_cpio_kms_client_interface",
        "//cc/external:scp_cpio_kms_proto",
        "//cc/external:tink_cc_proto",
        "//cc/lookup_server/interface:lookup_server_interface_lib",
        "//cc/lookup_server/match_data_storage/src:match_data_storage",
        "//cc/lookup_server/parsers/src:parsers_lib",
        "//protos/core:cc_proto",
        "//protos/lookup_server/backend:cc_proto",
    ],
)
