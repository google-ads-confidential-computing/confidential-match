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

package(default_visibility = [
    "//cc/core/data_table/src:__subpackages__",
    "//cc/core/data_table/test:__subpackages__",
])

cc_library(
    name = "data_table_internal_lib",
    srcs = glob(
        [
            "*.cc",
            "*.h",
        ],
    ),
    copts = [
        "-std=c++17",
    ],
    deps = [
        "//cc/core/data_table/src:data_table_error_codes",
        "//cc/external:absl_base_core_headers",
        "//cc/external:absl_hash",
        "//cc/external:absl_span",
        "//cc/external:absl_str_format",
        "//cc/external:absl_strings",
        "//cc/external:absl_synchronization",
        "//cc/external:google_protobuf",
        "//cc/external:onetbb",
        "//cc/external:scp_core_global_logger_lib",
        "//cc/external:scp_core_interface_lib",
        "//protos/core:cc_proto",
    ],
)
