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
    name = "match_service_proto",
    srcs = ["match_service.proto"],
)

cc_proto_library(
    name = "match_service_cc_proto",
    deps = [":match_service_proto"],
)

java_proto_library(
    name = "match_service_java_proto",
    deps = [":match_service_proto"],
)

proto_library(
    name = "error_proto",
    srcs = ["error.proto"],
    deps = [
        "@com_google_googleapis//google/rpc:code_proto",
        "@com_google_protobuf//:descriptor_proto",
    ],
)

cc_proto_library(
    name = "error_cc_proto",
    deps = [":error_proto"],
)

cc_proto_library(
    name = "lookup_cc_proto",
    deps = [":lookup_proto"],
)

proto_library(
    name = "lookup_proto",
    srcs = ["lookup.proto"],
    deps = [
        ":match_service_proto",
    ],
)

cc_proto_library(
    name = "private_key_endpoints_cc_proto",
    deps = [":private_key_endpoints_proto"],
)

proto_library(
    name = "private_key_endpoints_proto",
    srcs = ["private_key_endpoints.proto"],
    deps = [
        "//cc/external:scp_cpio_private_key_service_proto",
    ],
)

cc_proto_library(
    name = "key_fetcher_options_cc_proto",
    deps = [":key_fetcher_options_proto"],
)

proto_library(
    name = "key_fetcher_options_proto",
    srcs = ["key_fetcher_options.proto"],
    deps = [
        "//cc/external:scp_cpio_encryption_key_prefetch_config_proto",
        "@com_google_protobuf//:duration_proto",
    ],
)
