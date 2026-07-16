# Copyright 2026 Google LLC
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
    name = "match_service_layer",
    srcs = ["//cc/match_service/server:match_server"],
    include_runfiles = True,
    package_dir = "/opt/google/match_service",
    tags = ["manual"],
)

# Bundles ca-certificates as /etc/ssl/certs/ca-certificates.crt
# Images must set the environment variable SSL_CERT_FILE to this full file path
# https://registry.bazel.build/docs/rules_distroless/0.8.0#rule-cacerts
cacerts(
    name = "cacerts",
    package = "@match_service_apt//ca-certificates/amd64:data",
    tags = ["manual"],
)

# Restores base image config values that were removed during flattening
oci_image(
    name = "match_service_gcp_image",
    base = "@debian12_base",
    entrypoint = ["/opt/google/match_service/match_server"],
    env = {
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "enabled_log_levels": "Info,Warning,Error,Critical",
        "host_address": "[::]",
        "host_port": "50051",
        # Required to use :cacerts, see that target definition for details
        "SSL_CERT_FILE": "/etc/ssl/certs/ca-certificates.crt",
        "enable_metric_client": "false",
    },
    exposed_ports = [
        "50051/tcp",
        "50052/tcp",
    ],
    labels = {
        "tee.launch_policy.allow_cmd_override": "false",
        "tee.launch_policy.allow_env_override": ",".join([
            "enable_metric_client",
            "metric_namespace",
            "collector_address",
            "cpu_async_executor_queue_size",
            "cpu_async_executor_threads_count",
            "io_async_executor_queue_size",
            "io_async_executor_threads_count",
            "match_service_queue_size",
            "match_service_thread_count",
            "gcp_project_id",
            "aws_kms_default_audience",
            "aws_kms_default_signatures",
            "orchestrator_host_address",
            "lookup_service_auth_audience",
            "lookup_service_cluster_group_id",
            "allowed_client_accounts",
        ]),
        "tee.launch_policy.log_redirect": "always",
        "tee.launch_policy.monitoring_memory_allow": "always",
    },
    tags = ["manual"],
    # Tar layers are ordered from least-to-most likely to change between builds
    tars = [
        ":cacerts",
        "@match_service_apt//:flat",
        ":match_service_layer",
    ],
    user = "0",
    workdir = "/",
)

oci_load(
    name = "match_service_gcp_image_load",
    image = ":match_service_gcp_image",
    repo_tags = ["bazel/cc/match_service/deploy:match_service_gcp_image"],
    tags = ["manual"],
)
