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

load("@rules_oci//oci:defs.bzl", "oci_load")

package(default_visibility = ["//visibility:public"])

oci_load(
    name = "java_base",
    testonly = True,
    image = "@java_base",
    repo_tags = ["bazel/javatests/com/google/cm/testutils/gcp:java_base"],
    tags = ["manual"],
)

filegroup(
    name = "java_base.tar",
    srcs = [":java_base"],
    output_group = "tarball",
    tags = ["manual"],
)

oci_load(
    name = "gcs_emulator",
    testonly = True,
    image = "@gcs_emulator",
    repo_tags = ["bazel/javatests/com/google/cm/testutils/gcp:gcs_emulator"],
    tags = ["manual"],
)

filegroup(
    name = "gcs_emulator.tar",
    srcs = [":gcs_emulator"],
    output_group = "tarball",
    tags = ["manual"],
)

oci_load(
    name = "pubsub_emulator",
    testonly = True,
    image = "@pubsub_emulator",
    repo_tags = ["bazel/javatests/com/google/cm/testutils/gcp:pubsub_emulator"],
    tags = ["manual"],
)

filegroup(
    name = "pubsub_emulator.tar",
    srcs = [":pubsub_emulator"],
    output_group = "tarball",
    tags = ["manual"],
)

oci_load(
    name = "spanner_emulator",
    testonly = True,
    image = "@spanner_emulator",
    repo_tags = ["bazel/javatests/com/google/cm/testutils/gcp:spanner_emulator"],
    tags = ["manual"],
)

filegroup(
    name = "spanner_emulator.tar",
    srcs = [":spanner_emulator"],
    output_group = "tarball",
    tags = ["manual"],
)

java_library(
    name = "gcp",
    testonly = True,
    srcs = glob(["*.java"]),
    data = [
        ":gcs_emulator.tar",
        ":java_base.tar",
        ":pubsub_emulator.tar",
        ":spanner_emulator.tar",
    ],
    runtime_deps = ["//java/external:gcp_function_invoker"],
    deps = [
        "//java/external:gcp_core",
        "//java/external:gcp_pubsub",
        "//java/external:gcp_pubsub_proto",
        "//java/external:gcp_spanner",
        "//java/external:gcp_storage",
        "//java/external:google_gax",
        "//java/external:google_gax_grpc",
        "//java/external:grpc_api",
        "//java/external:guava",
        "//java/external:guice",
        "//java/external:testcontainers",
    ],
)
