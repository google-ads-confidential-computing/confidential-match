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

load("@rules_distroless//distroless:defs.bzl", "cacerts")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_load")
load("@rules_pkg//:pkg.bzl", "pkg_tar")

pkg_tar(
    name = "lookup_service_layer",
    srcs = ["//cc/lookup_server/server/src:lookup_server"],
    include_runfiles = True,
    package_dir = "opt/google/lookup_service",
    tags = ["manual"],
)

pkg_tar(
    name = "startup_script_layer",
    srcs = [":startup_script.sh"],
    tags = ["manual"],
)

# Bundles ca-certificates as /etc/ssl/certs/ca-certificates.crt
# Images must set the environment variable SSL_CERT_FILE to this full file path
# https://registry.bazel.build/docs/rules_distroless/0.8.0#rule-cacerts
cacerts(
    name = "cacerts",
    package = "@lookup_server_apt//ca-certificates/amd64:data",
    tags = ["manual"],
)

# Restores base image config values that were removed during flattening
oci_image(
    name = "lookup_server_gcp_image_4_vcpu",
    base = "@debian12_base",
    entrypoint = ["/startup_script.sh"],
    env = {
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "enabled_log_levels": "Debug,Info,Warning,Error,Critical",
        "async_executor_queue_size": "1000",
        "async_executor_threads_count": "4",  # For n2d-standard-4
        "io_async_executor_queue_size": "1000",
        "io_async_executor_threads_count": "50",
        # Required to use :cacerts, see that target definition for details
        "SSL_CERT_FILE": "/etc/ssl/certs/ca-certificates.crt",
    },
    exposed_ports = [
        "8080/tcp",
        "8081/tcp",
    ],
    labels = {
        "tee.launch_policy.allow_cmd_override": "false",
        "tee.launch_policy.allow_env_override": "cluster_id,cluster_group_id,environment",
        "tee.launch_policy.log_redirect": "always",
        "tee.launch_policy.monitoring_memory_allow": "always",
    },
    tags = ["manual"],
    # Tar layers are ordered from least-to-most likely to change between builds
    tars = [
        ":startup_script_layer",
        ":cacerts",
        "@lookup_server_apt//:flat",
        ":lookup_service_layer",
    ],
    user = "0",
    workdir = "/",
)

oci_load(
    name = "lookup_server_gcp_image_4_vcpu_load",
    image = ":lookup_server_gcp_image_4_vcpu",
    repo_tags = ["bazel/cc/lookup_server/deploy:lookup_server_gcp_image_4_vcpu"],
    tags = ["manual"],
)

# Restores base image config values that were removed during flattening
oci_image(
    name = "lookup_server_gcp_image_48_vcpu",
    base = "@debian12_base",
    entrypoint = ["/startup_script.sh"],
    env = {
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "enabled_log_levels": "Info,Warning,Error,Critical",
        "async_executor_queue_size": "100000",
        "async_executor_threads_count": "48",  # For n2d-highmem-48
        "io_async_executor_queue_size": "100000",
        "io_async_executor_threads_count": "500",
        # Required to use :cacerts, see that target definition for details
        "SSL_CERT_FILE": "/etc/ssl/certs/ca-certificates.crt",
    },
    exposed_ports = [
        "8080/tcp",
        "8081/tcp",
    ],
    labels = {
        "tee.launch_policy.allow_cmd_override": "false",
        "tee.launch_policy.allow_env_override": "cluster_id,cluster_group_id,environment",
        "tee.launch_policy.log_redirect": "always",
        "tee.launch_policy.monitoring_memory_allow": "always",
    },
    tags = ["manual"],
    # Tar layers are ordered from least-to-most likely to change between builds
    tars = [
        ":startup_script_layer",
        ":cacerts",
        "@lookup_server_apt//:flat",
        ":lookup_service_layer",
    ],
    user = "0",
    workdir = "/",
)

oci_load(
    name = "lookup_server_gcp_image_48_vcpu_load",
    image = ":lookup_server_gcp_image_48_vcpu",
    repo_tags = ["bazel/cc/lookup_server/deploy:lookup_server_gcp_image_48_vcpu"],
    tags = ["manual"],
)
