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
    name = "lookup_service_lib",
    srcs = glob(
        [
            "*.cc",
            "*.h",
        ],
    ),
    copts = [
        "-std=c++17",
    ],
    linkopts = [
        "-latomic",
    ],
    deps = [
        "//cc/external:absl_flat_hash_map",
        "//cc/external:absl_status",
        "//cc/external:absl_strings",
        "//cc/external:absl_time",
        "//cc/external:google_protobuf",
        "//cc/external:grpc_proto_converter",
        "//cc/external:scp_core_async_context_lib",
        "//cc/external:scp_core_common_serialization",
        "//cc/external:scp_core_errors_lib",
        "//cc/external:scp_core_interface_lib",
        "//cc/external:scp_cpio_metric_client_interface",
        "//cc/external:scp_cpio_metric_instance",
        "//cc/external:scp_cpio_metric_instance_interface",
        "//cc/external:scp_cpio_metric_proto",
        "//cc/lookup_server/converters/src:converters_lib",
        "//cc/lookup_server/interface:lookup_server_interface_lib",
        "//cc/lookup_server/public/src:errors_lib",
        "//protos/lookup_server/api:cc_proto",
        "//protos/lookup_server/backend:cc_proto",
        "//protos/shared/api/errors:cc_proto",
    ],
)
