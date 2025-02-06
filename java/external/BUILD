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

load("@rules_java//java:defs.bzl", "java_library", "java_plugin")

package(default_visibility = ["//visibility:public"])

################################################################################
# General Use Libraries: Begin
################################################################################

alias(
    name = "acai",
    actual = "@maven//:com_google_acai_acai",
)

alias(
    name = "autovalue_annotations",
    actual = "@maven//:com_google_auto_value_auto_value_annotations",
)

alias(
    name = "autovalue_processor",
    actual = "@maven//:com_google_auto_value_auto_value",
)

java_plugin(
    name = "autovalue_plugin",
    processor_class = "com.google.auto.value.processor.AutoValueProcessor",
    deps = [":autovalue_processor"],
)

java_plugin(
    name = "autooneof_plugin",
    processor_class = "com.google.auto.value.processor.AutoOneOfProcessor",
    deps = [":autovalue_processor"],
)

java_library(
    name = "autovalue",
    exported_plugins = [
        ":autooneof_plugin",
        ":autovalue_plugin",
    ],
    neverlink = True,
    exports = [":autovalue_annotations"],
)

alias(
    name = "google_http_client",
    actual = "@maven//:com_google_http_client_google_http_client",
)

alias(
    name = "findbugs",
    actual = "@maven//:com_google_code_findbugs_jsr305",
)

alias(
    name = "google_http_client_gson",
    actual = "@maven//:com_google_http_client_google_http_client_gson",
)

alias(
    name = "apache_httpclient5",
    actual = "@maven//:org_apache_httpcomponents_client5_httpclient5",
)

alias(
    name = "apache_httpcore5",
    actual = "@maven//:org_apache_httpcomponents_core5_httpcore5",
)

alias(
    name = "apache_httpcore5_h2",
    actual = "@maven//:org_apache_httpcomponents_core5_httpcore5_h2",
)

alias(
    name = "google_gax",
    actual = "@maven//:com_google_api_gax",
)

alias(
    name = "google_gax_grpc",
    actual = "@maven//:com_google_api_gax_grpc",
)

alias(
    name = "guava",
    actual = "@maven//:com_google_guava_guava",
)

alias(
    name = "guice",
    actual = "@maven//:com_google_inject_guice",
)

alias(
    name = "guice-assistedinject",
    actual = "@maven//:com_google_inject_extensions_guice_assistedinject",
)

alias(
    name = "grpc_api",
    actual = "@maven//:io_grpc_grpc_api",
)

alias(
    name = "jackson_annotations",
    actual = "@maven//:com_fasterxml_jackson_core_jackson_annotations",
)

alias(
    name = "jackson_core",
    actual = "@maven//:com_fasterxml_jackson_core_jackson_core",
)

alias(
    name = "jackson_databind",
    actual = "@maven//:com_fasterxml_jackson_core_jackson_databind",
)

alias(
    name = "javax_inject",
    actual = "@maven//:javax_inject_javax_inject",
)

alias(
    name = "jcommander",
    actual = "@maven//:com_beust_jcommander",
)

alias(
    name = "slf4j",
    actual = "@maven//:org_slf4j_slf4j_api",
)

alias(
    name = "slf4j_simple",
    actual = "@maven//:org_slf4j_slf4j_simple",
)

alias(
    name = "protobuf_java",
    actual = "@maven//:com_google_protobuf_protobuf_java",
)

alias(
    name = "protobuf_java_util",
    actual = "@maven//:com_google_protobuf_protobuf_java_util",
)

alias(
    name = "google_truth",
    actual = "@maven//:com_google_truth_truth",
)

alias(
    name = "google_truth_proto",
    actual = "@maven//:com_google_truth_extensions_truth_proto_extension",
)

alias(
    name = "google_truth8",
    actual = "@maven//:com_google_truth_extensions_truth_java8_extension",
)

alias(
    name = "apache_commons_csv",
    actual = "@maven//:org_apache_commons_commons_csv",
)

alias(
    name = "apache_commons_lang",
    actual = "@maven//:org_apache_commons_commons_lang3",
)

alias(
    name = "resilience4j_core",
    actual = "@maven//:io_github_resilience4j_resilience4j_core",
)

alias(
    name = "resilience4j_retry",
    actual = "@maven//:io_github_resilience4j_resilience4j_retry",
)

alias(
    name = "mockito",
    actual = "@maven//:org_mockito_mockito_core",
    visibility = ["//javatests:__subpackages__"],
)

alias(
    name = "mockito_inline",
    actual = "@maven//:org_mockito_mockito_inline",
    visibility = ["//javatests:__subpackages__"],
)

alias(
    name = "testcontainers",
    actual = "@maven//:org_testcontainers_testcontainers",
)

alias(
    name = "tink",
    actual = "@maven//:com_google_crypto_tink_tink",
)

alias(
    name = "tink-gcpkms",
    actual = "@maven//:com_google_crypto_tink_tink_gcpkms",
)

alias(
    name = "conscrypt_openjdk_uber",
    actual = "@maven//:org_conscrypt_conscrypt_openjdk_uber",
)

alias(
    name = "mockserver_core",
    actual = "@maven//:org_mock_server_mockserver_core",
)

alias(
    name = "mockserver_client",
    actual = "@maven//:org_mock_server_mockserver_client_java",
)

alias(
    name = "mockserver_junit_rule",
    actual = "@maven//:org_mock_server_mockserver_junit_rule",
)

alias(
    name = "junixsocket-core",
    actual = "@maven//:com_kohlschutter_junixsocket_junixsocket_core",
)

################################################################################
# General Use Libraries: End
################################################################################
################################################################################
# Cloud Libraries: Begin
################################################################################

alias(
    name = "gcp_core",
    actual = "@maven//:com_google_cloud_google_cloud_core",
)

alias(
    name = "gcp_spanner",
    actual = "@maven//:com_google_cloud_google_cloud_spanner",
)

alias(
    name = "gcp_cloud_function",
    actual = "@maven//:com_google_cloud_functions_functions_framework_api",
)

alias(
    name = "gcp_function_invoker",
    actual = "@maven//:com_google_cloud_functions_invoker_java_function_invoker",
)

alias(
    name = "gcp_monitoring",
    actual = "@maven//:com_google_cloud_google_cloud_monitoring",
)

alias(
    name = "gcp_monitoring_proto",
    actual = "@maven//:com_google_api_grpc_proto_google_cloud_monitoring_v3",
)

alias(
    name = "gcp_storage",
    actual = "@maven//:com_google_cloud_google_cloud_storage",
)

alias(
    name = "gcp_pubsub",
    actual = "@maven//:com_google_cloud_google_cloud_pubsub",
)

alias(
    name = "gcp_pubsub_proto",
    actual = "@maven//:com_google_api_grpc_proto_google_cloud_pubsub_v1",
)

alias(
    name = "google_auth_library_oauth2_http",
    actual = "@maven//:com_google_auth_google_auth_library_oauth2_http",
)

alias(
    name = "google_auth_library_credentials",
    actual = "@maven//:com_google_auth_google_auth_library_credentials",
)

alias(
    name = "google_api_client",
    actual = "@maven//:com_google_api_client_google_api_client",
)

alias(
    name = "google_common_proto",
    actual = "@maven//:com_google_api_grpc_proto_google_common_protos",
)

################################################################################
# Cloud Libraries: End
################################################################################
################################################################################
# SCP Libraries: Begin
################################################################################

alias(
    name = "clients_blobstorageclient",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/operator/cpio/blobstorageclient",
)

alias(
    name = "clients_blobstorageclient_gcp",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/operator/cpio/blobstorageclient:gcp",
)

alias(
    name = "clients_blobstorageclient_model",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/operator/cpio/blobstorageclient:model",
)

alias(
    name = "clients_configclient",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/shared/clients/configclient",
)

alias(
    name = "clients_configclient_gcp",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/shared/clients/configclient:gcp",
)

alias(
    name = "clients_configclient_model",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/shared/clients/configclient:model",
)

alias(
    name = "clients_cryptoclient",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/operator/cpio/cryptoclient",
)

alias(
    name = "clients_jobclient",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/operator/cpio/jobclient",
)

alias(
    name = "clients_jobclient_gcp",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/operator/cpio/jobclient:gcp",
)

alias(
    name = "clients_jobclient_model",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/operator/cpio/jobclient:model",
)

alias(
    name = "clients_metricclient",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/operator/cpio/metricclient",
)

alias(
    name = "clients_metricclient_gcp",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/operator/cpio/metricclient:gcp",
)

alias(
    name = "clients_metricclient_model",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/operator/cpio/metricclient:model",
)

alias(
    name = "clients_lifecycleclient_gcp",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/operator/cpio/lifecycleclient:gcp",
)

alias(
    name = "clients_lifecycleclient_local",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/operator/cpio/lifecycleclient:local",
)

alias(
    name = "clients_notificationclient_gcp",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/operator/cpio/notificationclient:gcp",
)

alias(
    name = "clients_notificationclient_local",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/operator/cpio/notificationclient:local",
)

alias(
    name = "operator_dao_jobqueue",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/operator/shared/dao/jobqueue/common",
)

alias(
    name = "operator_dao_jobqueue_gcp",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/operator/shared/dao/jobqueue/gcp",
)

alias(
    name = "operator_dao_metadatadb",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/operator/shared/dao/metadatadb/common",
)

alias(
    name = "operator_dao_metadatadb_gcp",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/operator/shared/dao/metadatadb/gcp",
)

alias(
    name = "operator_frontend_service",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/operator/frontend/service",
)

alias(
    name = "operator_frontend_service_gcp",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/operator/frontend/service/gcp",
)

alias(
    name = "operator_frontend_service_converter",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/operator/frontend/service/converter",
)

alias(
    name = "operator_frontend_tasks_gcp",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/operator/frontend/tasks/gcp",
)

alias(
    name = "shared_api_exception",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/shared/api/exception",
)

alias(
    name = "shared_api_model",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/shared/api/model",
)

alias(
    name = "shared_crypto_tink",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/shared/crypto/tink",
)

alias(
    name = "shared_gcp_util",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/shared/gcp/util",
)

alias(
    name = "shared_mapper",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/shared/mapper",
)

alias(
    name = "shared_proto_util",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/shared/proto",
)

alias(
    name = "shared_util",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/shared/util",
)

alias(
    name = "shared_api_util",
    actual = "@com_google_adm_cloud_scp//java/com/google/scp/shared/api/util",
)

alias(
    name = "shared_proto",
    actual = "@com_google_adm_cloud_scp//shared/protos/api/v1:java_proto",
)

alias(
    name = "operator_backend_protos",
    actual = "@com_google_adm_cloud_scp//operator/protos/shared/backend:java_proto",
)

alias(
    name = "operator_backend_metadatadb_protos",
    actual = "@com_google_adm_cloud_scp//operator/protos/shared/backend/metadatadb:java_proto",
)

alias(
    name = "operator_frontend_protos",
    actual = "@com_google_adm_cloud_scp//operator/protos/frontend/api/v1:java_proto",
)

################################################################################
# SCP Libraries: End
################################################################################
