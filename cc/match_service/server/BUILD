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

load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_load")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")

package(default_visibility = ["//visibility:public"])

pkg_tar(
    name = "binary_layer",
    srcs = [":match_server"],
    include_runfiles = True,
    package_dir = "/usr/local/bin/",
    tags = ["manual"],
)

oci_image(
    name = "image",
    base = "@debian12_distroless",
    entrypoint = ["/usr/local/bin/match_server"],
    env = {
        "enabled_log_levels": "Info,Warning,Error,Critical",
        "host_address": "[::]",
        "host_port": "50051",
    },
    exposed_ports = [
        "50051/tcp",
        "50052/tcp",
    ],
    labels = {
        "tee.launch_policy.allow_cmd_override": "false",
        "tee.launch_policy.allow_env_override": ",".join([
            "cpu_async_executor_threads_count",
            "cpu_async_executor_queue_size",
            "io_async_executor_threads_count",
            "io_async_executor_queue_size",
            "gcp_project_id",
            "allowed_client_accounts",
        ]),
        "tee.launch_policy.log_redirect": "always",
        "tee.launch_policy.monitoring_memory_allow": "always",
    },
    tags = ["manual"],
    tars = [":binary_layer"],
)

oci_load(
    name = "load_image",
    image = ":image",
    repo_tags = ["confidential-match/match_service:local"],
    tags = ["manual"],
)

cc_binary(
    name = "match_server",
    srcs = ["server_runner_main.cc"],
    deps = [
        ":match_server_lib",
        "//cc/external:absl_check",
        "//cc/external:absl_debugging_failure_signal_handler",
        "//cc/external:absl_debugging_symbolize",
        "//cc/external:absl_log_initialize",
        "//cc/external:absl_status",
        "//cc/external:scp_core_config_provider",
        "//cc/external:scp_core_global_logger_lib",
        "//cc/external:scp_core_interface_lib",
        "//cc/external:scp_core_log_providers",
        "//cc/external:scp_core_logger",
        "//cc/external:scp_cpio_metric_instance",
        "//cc/external:scp_cpio_metric_instance_interface",
    ],
)

cc_library(
    name = "match_server_lib",
    srcs = ["match_server.cc"],
    hdrs = ["match_server.h"],
    deps = [
        "//cc/core/error:status_macros",
        "//cc/core/hash:hasher_interface",
        "//cc/core/hash:sha256_hasher",
        "//cc/external:grpc++",
        "//cc/external:grpc++_reflection",
        "//cc/external:scp_core_async_executor",
        "//cc/external:scp_core_authorization_proxy_lib",
        "//cc/external:scp_core_errors_lib",
        "//cc/external:scp_core_execution_result",
        "//cc/external:scp_core_http1_curl_client",
        "//cc/external:scp_core_http2_client",
        "//cc/external:scp_core_http2_server",
        "//cc/external:scp_core_interface_lib",
        "//cc/external:scp_cpio_aws_kms_client",
        "//cc/external:scp_cpio_cloud_initializer",
        "//cc/external:scp_cpio_configuration_fetcher",
        "//cc/external:scp_cpio_dual_writing_metric_client",
        "//cc/external:scp_cpio_interface_lib",
        "//cc/external:scp_cpio_key_fetcher_interface",
        "//cc/external:scp_cpio_metric_client_interface",
        "//cc/external:scp_cpio_noop_dual_writing_metric_client",
        "//cc/external:scp_cpio_ondemand_key_fetcher",
        "//cc/external:scp_cpio_private_key_client_interface",
        "//cc/match_service/auth_token_client",
        "//cc/match_service/auth_token_client:auth_token_client_interface",
        "//cc/match_service/auth_token_client:cached_auth_token_client",
        "//cc/match_service/crypto_client:aead_crypto_client",
        "//cc/match_service/crypto_client:crypto_client_interface",
        "//cc/match_service/crypto_client:hybrid_crypto_client",
        "//cc/match_service/kms_client",
        "//cc/match_service/kms_client:cached_kms_client",
        "//cc/match_service/kms_client:kms_client_interface",
        "//cc/match_service/lookup_service_client",
        "//cc/match_service/lookup_service_client:lookup_service_client_interface",
        "//cc/match_service/lookup_service_shard_client",
        "//cc/match_service/lookup_service_shard_client:lookup_service_shard_client_interface",
        "//cc/match_service/orchestrator_client",
        "//cc/match_service/orchestrator_client:cached_orchestrator_client",
        "//cc/match_service/orchestrator_client:orchestrator_client_interface",
        "//cc/match_service/service:match_service",
        "//cc/match_service/tasks:hashed_match_task",
        "//cc/match_service/tasks:match_task",
        "//cc/match_service/tasks:match_task_interface",
        "//protos/match_service/backend:key_fetcher_options_cc_proto",
        "//protos/match_service/backend:private_key_endpoints_cc_proto",
        "//protos/match_service/backend/configuration:configuration_keys_cc_proto",
    ],
)
