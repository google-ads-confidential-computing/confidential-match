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

load("@io_bazel_rules_docker//container:container.bzl", "container_image")

package(default_visibility = ["//visibility:public"])

container_image(
    name = "java_base",
    testonly = True,
    base = "@java_base//image",
)

container_image(
    name = "gcs_emulator",
    testonly = True,
    base = "@gcs_emulator//image",
)

container_image(
    name = "pubsub_emulator",
    testonly = True,
    base = "@pubsub_emulator//image",
)

container_image(
    name = "spanner_emulator",
    testonly = True,
    base = "@spanner_emulator//image",
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
