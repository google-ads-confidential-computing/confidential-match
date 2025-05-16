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

load("@io_bazel_rules_docker//container:container.bzl", "container_image", "container_push")

package(default_visibility = ["//visibility:public"])

java_library(
    name = "matchworker_runner",
    srcs = glob(["*.java"]),
    resources = [
        "configs/adh_config.json",
        "configs/copla_config.json",
        "configs/customer_match_config.json",
        "configs/dm_api_config.json",
        "configs/mic_audience_config.json",
        "configs/mic_config.json",
    ],
    deps = [
        "//java/com/google/cm/mrp/clients/attestation",
        "//java/com/google/cm/mrp/clients/cryptoclient",
        "//java/com/google/cm/mrp/clients/cryptoclient/aws",
        "//java/com/google/cm/mrp/clients/cryptoclient/gcp",
        "//java/com/google/cm/mrp/clients/lookupserviceclient/gcp",
        "//java/com/google/cm/mrp/dataprocessor",
        "//java/com/google/cm/mrp/dataprocessor/converters",
        "//java/com/google/cm/mrp/dataprocessor/models",
        "//java/com/google/cm/mrp/models",
        "//java/com/google/cm/mrp/selectors",
        "//java/com/google/cm/util",
        "//java/external:autovalue",
        "//java/external:clients_blobstorageclient_gcp",
        "//java/external:clients_configclient_gcp",
        "//java/external:clients_jobclient_gcp",
        "//java/external:clients_jobclient_model",
        "//java/external:clients_metricclient",
        "//java/external:clients_metricclient_model",
        "//java/external:conscrypt_openjdk_uber",
        "//java/external:guava",
        "//java/external:guice",
        "//java/external:guice-assistedinject",
        "//java/external:jackson_databind",
        "//java/external:javax_inject",
        "//java/external:jcommander",
        "//java/external:log4j_api",
        "//java/external:log4j_core",
        "//java/external:operator_backend_protos",
        "//java/external:operator_frontend_protos",
        "//java/external:protobuf_java",
        "//java/external:protobuf_java_util",
        "//java/external:resilience4j_retry",
        "//java/external:shared_mapper",
        "//java/external:shared_proto_util",
        "//java/external:slf4j",
        "//java/external:tink",
        "//protos/mrp/api:java_proto",
        "//protos/mrp/backend:java_proto",
    ],
)

java_library(
    name = "exceptions",
    srcs = glob(["JobProcessorException.java"]),
    deps = [
        "//protos/mrp/backend:java_proto",
    ],
)

java_library(
    name = "constants",
    srcs = ["Constants.java"],
)

# TODO(b/376719449): refactor to package

java_library(
    name = "featureFlags",
    srcs = glob(["FeatureFlags.java"]),
    deps = [
        "//java/external:autovalue",
    ],
)

java_library(
    name = "startup_config",
    srcs = glob(["StartupConfig.java"]),
    deps = [
        "//java/external:autovalue",
        "//java/external:guava",
    ],
)

java_library(
    name = "match_config_provider",
    srcs = ["MatchConfigProvider.java"],
    resources = [
        "configs/adh_config.json",
        "configs/copla_config.json",
        "configs/customer_match_config.json",
        "configs/mic_audience_config.json",
        "configs/mic_config.json",
    ],
    deps = [
        "//java/com/google/cm/mrp:exceptions",
        "//java/com/google/cm/util",
        "//java/external:guava",
        "//java/external:slf4j",
        "//protos/mrp/backend:java_proto",
    ],
)

filegroup(
    name = "log4j2config_prod",
    srcs = ["log4j2.properties"],
)

java_binary(
    name = "MatchWorkerRunner",
    classpath_resources = ["//java/com/google/cm/mrp:log4j2config_prod"],
    main_class = "com.google.cm.mrp.MatchWorkerApplication",
    runtime_deps = [
        ":matchworker_runner",
        "//java/external:log4j_core",
        "//java/external:log4j_slf4j2_impl",
    ],
)

alias(
    name = "MatchWorkerRunnerDeploy",
    actual = ":MatchWorkerRunner_deploy.jar",
)

container_image(
    name = "mrp_app_gcp_image",
    base = "@java_base//image",
    cmd = [
        "MatchWorkerRunner_deploy.jar",
        "--blob_storage_client",
        "GCS",
        "--client_config_env",
        "GCP",
        "--job_client",
        "GCP",
        "--lifecycle_client",
        "GCP",
        "--metric_client",
        "GCP",
        "--param_client",
        "GCP",
        "--notification_client",
        "GCP",
    ],
    entrypoint = [
        "/usr/bin/java",
        "-XX:MaxRAMPercentage=66.0",
        "-jar",
    ],
    files = ["//java/com/google/cm/mrp:MatchWorkerRunnerDeploy"],
    labels = {
        "tee.launch_policy.allow_cmd_override": "false",
        "tee.launch_policy.log_redirect": "always",
        "tee.launch_policy.monitoring_memory_allow": "always",
    },
)
