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

load("@com_google_protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("@com_google_protobuf//bazel:java_proto_library.bzl", "java_proto_library")
load("@com_google_protobuf//bazel:proto_library.bzl", "proto_library")

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "proto",
    srcs = glob(
        ["*.proto"],
        exclude = [
            "coordinator_set_configurations.proto",
        ],
    ),
    deps = [
        "//protos/core:proto",
    ],
)

cc_proto_library(
    name = "cc_proto",
    deps = [":proto"],
)

java_proto_library(
    name = "java_proto",
    deps = [":proto"],
)

proto_library(
    name = "coordinator_set_configurations_proto",
    srcs = ["coordinator_set_configurations.proto"],
    deps = [
        "//cc/external:scp_cpio_encryption_key_prefetch_config_proto",
        "//cc/external:scp_cpio_key_coordinator_configuration_proto",
        "@com_google_protobuf//:duration_proto",
    ],
)

cc_proto_library(
    name = "coordinator_set_configurations_cc_proto",
    deps = [":coordinator_set_configurations_proto"],
)
