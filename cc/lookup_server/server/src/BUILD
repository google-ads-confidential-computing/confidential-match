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

load("@bazel_skylib//rules:common_settings.bzl", "string_flag")

package(default_visibility = ["//cc/lookup_server:__subpackages__"])

cloud_platform_copts = select({
    "cloud_platform_gcp": [
        "-DCLOUD_PLATFORM_GCP=1",
    ],
    "cloud_platform_gcp_with_test_cpio": [
        "-DCLOUD_PLATFORM_GCP_WITH_TEST_CPIO=1",
    ],
    "cloud_platform_gcp_integration_test": [
        "-DCLOUD_PLATFORM_GCP_INTEGRATION_TEST=1",
    ],
    "cloud_platform_local": [
        "-DCLOUD_PLATFORM_LOCAL=1",
    ],
    "//conditions:default": [],
})

cc_binary(
    name = "lookup_server",
    srcs = [
        "server_runner_main.cc",
    ],
    copts =
        [
            "-std=c++17",
        ],
    linkopts = [
        # Tell LD to look in the working directory of the executable for SO
        # files to link
        "-Wl,-rpath,'$$ORIGIN'",
    ],
    malloc = "@jemalloc//:libjemalloc_static",
    deps = [
        ":lookup_server_lib",
        "//cc/external:absl_check",
        "//cc/external:absl_log",
        "//cc/external:absl_status",
        "//cc/external:absl_str_format",
        "//cc/external:absl_strings",
        "//cc/external:scp_core_common_uuid",
        "//cc/external:scp_core_config_provider",
        "//cc/external:scp_core_errors_lib",
        "//cc/external:scp_core_interface_lib",
        "//cc/external:scp_core_log_providers",
        "//cc/external:scp_core_logger",
        "//cc/external:scp_core_syslog",
    ],
)

lookup_server_lib_deps = [
    "//cc/lookup_server/interface:lookup_server_interface_lib",
    "//cc/lookup_server/auth/src:auth",
    "//cc/lookup_server/crypto_client/src:crypto_client",
    "//cc/lookup_server/kms_client/src:kms_client",
    "//cc/lookup_server/match_data_loader/src:match_data_loader",
    "//cc/lookup_server/match_data_provider/src:match_data_provider",
    "//cc/lookup_server/match_data_storage/src:match_data_storage",
    "//cc/lookup_server/metric_client/src:metric_client",
    "//cc/lookup_server/coordinator_client/src:coordinator_client",
    "//cc/lookup_server/orchestrator_client/src:orchestrator_client",
    "//cc/lookup_server/parameter_client/src:parameter_client",
    "//cc/lookup_server/service/src:lookup_service_lib",
    "//cc/lookup_server/health_service/src:health_service_lib",
    "//cc/external:absl_strings",
    "//cc/external:scp_core_async_executor",
    "//cc/external:scp_core_authorization_proxy_lib",
    "//cc/external:scp_core_common_uuid",
    "//cc/external:scp_core_http1_curl_client",
    "//cc/external:scp_core_http2_client",
    "//cc/external:scp_core_http2_server",
    "//cc/external:scp_core_config_provider",
    "//cc/external:scp_core_interface_lib",
    "//cc/external:scp_cpio_cloud_initializer",
    "//cc/external:scp_cpio_aws_kms_client",
    "//cc/external:scp_cpio_private_key_client_interface",
    "//cc/external:scp_cpio_blob_storage_client_interface",
    "//cc/external:scp_cpio_metric_client_interface",
    "//cc/external:scp_cpio_metric_instance_interface",
    "//cc/external:scp_cpio_parameter_client_interface",
]

lookup_server_lib_gcp_deps = lookup_server_lib_deps + [
    "//cc/lookup_server/server/src/cloud_platform_dependency_factory/gcp:lookup_server_gcp_cloud_platform_dependency_factory_lib",
    "//cc/external:scp_cpio_interface_lib",
]

lookup_server_lib_gcp_with_test_cpio_deps = lookup_server_lib_deps + [
    "//cc/lookup_server/server/src/cloud_platform_dependency_factory/gcp:lookup_server_gcp_cloud_platform_dependency_factory_lib",
    "//cc/external:scp_cpio_test_interface_lib",
]

lookup_server_lib_gcp_integration_test_deps = lookup_server_lib_deps + [
    "//cc/lookup_server/server/src/cloud_platform_dependency_factory/gcp:lookup_server_gcp_cloud_platform_dependency_factory_lib",
    "//cc/external:scp_cpio_interface_lib",
]

lookup_server_lib_local_deps = lookup_server_lib_deps + [
    "//cc/lookup_server/server/src/cloud_platform_dependency_factory/local:lookup_server_local_cloud_platform_dependency_factory_lib",
    "//cc/external:scp_cpio_interface_lib",
]

cc_library(
    name = "lookup_server_lib",
    srcs = glob(
        [
            "lookup_server.cc",
            "*.h",
        ],
    ),
    hdrs = [
        "error_codes.h",
        "lookup_server.h",
    ],
    copts = cloud_platform_copts + [
        "-std=c++17",
    ],
    deps = select({
        "cloud_platform_gcp": lookup_server_lib_gcp_deps,
        "cloud_platform_gcp_with_test_cpio": lookup_server_lib_gcp_with_test_cpio_deps,
        "cloud_platform_gcp_integration_test": lookup_server_lib_gcp_integration_test_deps,
        "cloud_platform_local": lookup_server_lib_local_deps,
        "//conditions:default": [],
    }),
)

string_flag(
    name = "platform",
    build_setting_default = "local",
    values = [
        "gcp",
        "gcp_with_test_cpio",
        "gcp_integration_test",
        "local",
    ],
)

config_setting(
    name = "cloud_platform_gcp",
    flag_values = {"platform": "gcp"},
)

config_setting(
    name = "cloud_platform_gcp_with_test_cpio",
    flag_values = {"platform": "gcp_with_test_cpio"},
)

config_setting(
    name = "cloud_platform_gcp_integration_test",
    flag_values = {"platform": "gcp_integration_test"},
)

config_setting(
    name = "cloud_platform_local",
    flag_values = {"platform": "local"},
)
