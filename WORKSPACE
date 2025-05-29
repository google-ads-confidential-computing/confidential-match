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

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository", "new_git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")

################################################################################
# Download all http_archives and git_repositories: Begin
################################################################################

SCP_VERSION = "v0.273.0"  # latest as of Wed Apr 16 14:41:25 2025 -0700

SCP_REPOSITORY = "https://github.com/google-ads-confidential-computing/conf-data-processing-architecture-reference"

git_repository(
    name = "com_google_adm_cloud_scp",
    remote = SCP_REPOSITORY,
    tag = SCP_VERSION,
)

################################################################################
# Rules JVM External: Begin
################################################################################
RULES_JVM_EXTERNAL_TAG = "4.4"
RULES_JVM_EXTERNAL_SHA = "0068b92a1527799d7268f6774654ed35024a660c6c68ac1f8011edade905929d"
http_archive(
    name = "rules_jvm_external",
    sha256 = RULES_JVM_EXTERNAL_SHA,
    strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
    url = "https://github.com/bazelbuild/rules_jvm_external/archive/%s.zip" % RULES_JVM_EXTERNAL_TAG,
)

load("@rules_jvm_external//:repositories.bzl", "rules_jvm_external_deps")

rules_jvm_external_deps()

load("@rules_jvm_external//:setup.bzl", "rules_jvm_external_setup")

rules_jvm_external_setup()

################################################################################
# Rules JVM External: End
################################################################################
################################################################################
# Download all http_archives and git_repositories: Begin
################################################################################

# Bazelisk for the Lookup Server builder image (file is named "downloaded")
http_file(
    name = "bazelisk",
    sha256 = "6539c12842ad76966f3d493e8f80d67caa84ec4a000e220d5459833c967c12bc",
    url = "https://github.com/bazelbuild/bazelisk/releases/download/v1.26.0/bazelisk-linux-amd64",
)

git_repository(
    name = "com_google_re2",
    # Committed on Mar 1, 2023
    # https://github.com/google/re2/commit/3a8436ac436124a57a4e22d5c8713a2d42b381d7
    commit = "3a8436ac436124a57a4e22d5c8713a2d42b381d7",
    remote = "https://github.com/google/re2.git",
)

################
# SDK Dependencies Rules #
################

# Declare explicit protobuf version and hash, to override any implicit dependencies.
# Please update both while upgrading to new versions.
PROTOBUF_CORE_VERSION = "28.0"

PROTOBUF_SHA_256 = "13e7749c30bc24af6ee93e092422f9dc08491c7097efa69461f88eb5f61805ce"
load("//build_defs/cc:cfm.bzl", "cfm_dependencies")

cfm_dependencies(PROTOBUF_CORE_VERSION, PROTOBUF_SHA_256)

####################
# Distroless rules #
####################

# Override a rules_distroless transitive dependency to avoid errors
http_archive(
    name = "aspect_bazel_lib",
    sha256 = "349aabd3c2b96caeda6181eb0ae1f14f2a1d9f3cd3c8b05d57f709ceb12e9fb3",
    strip_prefix = "bazel-lib-2.9.4",
    url = "https://github.com/bazel-contrib/bazel-lib/releases/download/v2.9.4/bazel-lib-v2.9.4.tar.gz",
)

# Bazel rules for managing OS packages
# https://github.com/GoogleContainerTools/rules_distroless/releases/tag/v0.3.9
http_archive(
    name = "rules_distroless",
    sha256 = "baa2b40100805dc3c2ad5dd1ab8555f1d19484f083a10d685c5812c8b7a78ad8",
    strip_prefix = "rules_distroless-0.3.9",
    url = "https://github.com/GoogleContainerTools/rules_distroless/releases/download/v0.3.9/rules_distroless-v0.3.9.tar.gz",
)

load("@rules_distroless//distroless:dependencies.bzl", "distroless_dependencies")

distroless_dependencies()

load("@rules_distroless//distroless:toolchains.bzl", "distroless_register_toolchains")

distroless_register_toolchains()

################################################################################
# Download all http_archives and git_repositories: End
################################################################################
################################################################################
# Download Indirect Dependencies: Begin
################################################################################
# Note: The order of statements in this section is extremely fragile

#############
# CPP Rules #
#############
load("@rules_cc//cc:repositories.bzl", "rules_cc_dependencies", "rules_cc_toolchains")

rules_cc_dependencies()

rules_cc_toolchains()

###########################
# CC Dependencies         #
###########################

# OpenTelemetry (Part 1/2)
# Add SCP's own newer opentelemetry before gcp c++ sdk
load("@com_google_adm_cloud_scp//build_defs/shared:opentelemetry.bzl", "opentelemetry_cpp")

opentelemetry_cpp()

# Load indirect dependencies due to
#     https://github.com/bazelbuild/bazel/issues/1943
load("@com_github_googleapis_google_cloud_cpp//bazel:google_cloud_cpp_deps.bzl", "google_cloud_cpp_deps")

google_cloud_cpp_deps()

load("@com_google_googleapis//:repository_rules.bzl", "switched_rules_by_language")

switched_rules_by_language(
    name = "com_google_googleapis_imports",
    cc = True,
    grpc = True,
)

# Boost
load("@com_github_nelhage_rules_boost//:boost/boost.bzl", "boost_deps")

boost_deps()

load("@rules_foreign_cc//foreign_cc:repositories.bzl", "rules_foreign_cc_dependencies")

rules_foreign_cc_dependencies()

##########
# GRPC C #
##########
# These dependencies from @com_github_grpc_grpc need to be loaded after the
# google cloud deps so that the grpc version can be set by the google cloud deps
load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")

grpc_deps()

load("@com_github_grpc_grpc//bazel:grpc_extra_deps.bzl", "grpc_extra_deps")

grpc_extra_deps()

bind(
    name = "cares",
    actual = "@com_github_cares_cares//:ares",
)

bind(
    name = "madler_zlib",
    actual = "@zlib//:zlib",
)
###############
# Proto rules #
###############
load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")

rules_proto_dependencies()

rules_proto_toolchains()

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

###########################
# CC Dependencies (cont.) #
###########################

# OpenTelemetry (Part 2/2)
# These are loaded separate from earlier dependencies to avoid gRPC errors
#
# (required after v1.8.0) Load extra dependencies required for OpenTelemetry
load("@io_opentelemetry_cpp//bazel:repository.bzl", "opentelemetry_cpp_deps")

opentelemetry_cpp_deps()

# (required after v1.8.0) Load extra dependencies required for OpenTelemetry
load("@io_opentelemetry_cpp//bazel:extra_deps.bzl", "opentelemetry_extra_deps")

opentelemetry_extra_deps()

################################################################################
# Download Indirect Dependencies: End
################################################################################

################################################################################
# Download Maven Dependencies: Begin
################################################################################
load("@rules_jvm_external//:defs.bzl", "maven_install")
load("@com_google_adm_cloud_scp//build_defs/tink:tink_defs.bzl", "TINK_MAVEN_ARTIFACTS")

JACKSON_VERSION = "2.12.2"

AUTO_VALUE_VERSION = "1.7.4"

AWS_SDK_VERSION = "2.17.239"

GOOGLE_GAX_VERSION = "2.47.0"

AUTO_SERVICE_VERSION = "1.0"

GUICE_VERSION = "5.1.0"  # latest as of Jan 25, 2022

RESILIENCE4J_VERSION = "1.7.1"

SLF4J_VERSION = "2.0.16"

maven_install(
    artifacts = [
        # Specify the protobuf-java explicitly to make sure
        # the version will be upgraded with protobuf cc.
        "com.google.protobuf:protobuf-java:4.28.0",
        "com.google.protobuf:protobuf-java-util:4.28.0",
        "com.google.protobuf:protobuf-javalite:4.28.0",
        "com.amazonaws:aws-lambda-java-core:1.2.1",
        "com.amazonaws:aws-lambda-java-events:3.8.0",
        "com.amazonaws:aws-lambda-java-events-sdk-transformer:3.1.0",
        "com.amazonaws:aws-java-sdk-sqs:1.11.860",
        "com.amazonaws:aws-java-sdk-s3:1.11.860",
        "com.amazonaws:aws-java-sdk-kms:1.11.860",
        "com.amazonaws:aws-java-sdk-core:1.11.860",
        "com.beust:jcommander:1.81",
        "com.fasterxml.jackson.core:jackson-annotations:" + JACKSON_VERSION,
        "com.fasterxml.jackson.core:jackson-core:" + JACKSON_VERSION,
        "com.fasterxml.jackson.core:jackson-databind:" + JACKSON_VERSION,
        "com.fasterxml.jackson.datatype:jackson-datatype-guava:" + JACKSON_VERSION,
        "com.fasterxml.jackson.datatype:jackson-datatype-jsr310:" + JACKSON_VERSION,
        "com.fasterxml.jackson.datatype:jackson-datatype-jdk8:" + JACKSON_VERSION,
        "com.google.acai:acai:1.1",
        "com.google.auto.factory:auto-factory:1.0",
        "com.google.auto.service:auto-service-annotations:" + AUTO_SERVICE_VERSION,
        "com.google.auto.service:auto-service:" + AUTO_SERVICE_VERSION,
        "com.google.auto.value:auto-value-annotations:" + AUTO_VALUE_VERSION,
        "com.google.auto.value:auto-value:" + AUTO_VALUE_VERSION,
        "com.google.code.findbugs:jsr305:3.0.2",
        "com.google.cloud:google-cloud-kms:2.48.0",
        "com.google.cloud:google-cloud-pubsub:1.132.0",
        "com.google.cloud:google-cloud-storage:2.41.0",
        "com.google.cloud:google-cloud-spanner:6.71.0",
        "com.google.cloud:google-cloud-secretmanager:2.46.0",
        "com.google.cloud:google-cloud-compute:1.57.0",
        "com.google.cloud:google-cloudevent-types:0.14.0",
        "com.google.api.grpc:proto-google-cloud-compute-v1:1.58.0",
        "com.google.api-client:google-api-client:2.7.0",
        "com.google.apis:google-api-services-cloudkms:v1-rev20240808-2.0.0",
        "com.google.cloud.functions.invoker:java-function-invoker:1.3.1",
        "com.google.auth:google-auth-library-oauth2-http:1.24.1",
        "com.google.cloud.functions:functions-framework-api:1.1.0",
        "commons-logging:commons-logging:1.1.1",
        "com.google.api:gax:" + GOOGLE_GAX_VERSION,
        "com.google.http-client:google-http-client:1.42.3",
        "com.google.http-client:google-http-client-gson:1.42.3",
        "com.google.cloud:google-cloud-monitoring:3.38.0",
        "com.google.api.grpc:proto-google-cloud-monitoring-v3:3.4.1",
        "com.google.api.grpc:proto-google-common-protos:2.27.0",
        "com.google.guava:guava:30.1-jre",
        "com.google.guava:guava-testlib:30.1-jre",
        "com.google.inject.extensions:guice-assistedinject:" + GUICE_VERSION,
        "com.google.inject:guice:" + GUICE_VERSION,
        "com.google.inject.extensions:guice-testlib:" + GUICE_VERSION,
        "com.google.jimfs:jimfs:1.2",
        "com.google.testparameterinjector:test-parameter-injector:1.1",
        "com.google.truth.extensions:truth-java8-extension:1.1.2",
        "com.google.truth.extensions:truth-proto-extension:1.1.2",
        "com.google.truth:truth:1.1.2",
        "com.jayway.jsonpath:json-path:2.5.0",
        "com.kohlschutter.junixsocket:junixsocket-core:2.10.1",
        "io.github.resilience4j:resilience4j-core:" + RESILIENCE4J_VERSION,
        "io.github.resilience4j:resilience4j-retry:" + RESILIENCE4J_VERSION,
        "io.opentelemetry:opentelemetry-api:1.31.0",
        "io.opentelemetry:opentelemetry-exporter-otlp:1.31.0",
        "io.opentelemetry:opentelemetry-sdk:1.31.0",
        "io.opentelemetry:opentelemetry-sdk-common:1.31.0",
        "io.opentelemetry:opentelemetry-sdk-metrics:1.31.0",
        "io.opentelemetry:opentelemetry-sdk-extension-autoconfigure:1.31.0",
        "com.google.cloud.opentelemetry:exporter-metrics:0.26.0",
        "com.google.cloud.opentelemetry:detector-resources:0.26.0-alpha",
        "javax.annotation:javax.annotation-api:1.3.2",
        "javax.inject:javax.inject:1",
        "junit:junit:4.12",
        "org.apache.avro:avro:1.10.2",
        "org.apache.commons:commons-csv:jar:1.10.0",
        "org.apache.commons:commons-lang3:3.14.0",
        "org.apache.commons:commons-math3:3.6.1",
        "org.apache.httpcomponents:httpcore:4.4.14",
        "org.apache.httpcomponents:httpclient:4.5.13",
        "org.apache.httpcomponents.client5:httpclient5:5.3.1",
        "org.apache.httpcomponents.core5:httpcore5:5.1.4",
        "org.apache.httpcomponents.core5:httpcore5-h2:5.1.4",  # Explicit transitive dependency to avoid https://issues.apache.org/jira/browse/HTTPCLIENT-2222
        "org.apache.logging.log4j:log4j-api:2.19.0",
        "org.apache.logging.log4j:log4j-core:2.19.0",
        "org.apache.logging.log4j:log4j-slf4j2-impl:2.19.0",
        "org.awaitility:awaitility:3.0.0",
        "org.conscrypt:conscrypt-openjdk-uber:2.5.2",
        "org.mock-server:mockserver-core:5.15.0",
        "org.mock-server:mockserver-junit-rule:5.15.0",
        "org.mock-server:mockserver-client-java:5.15.0",
        "org.hamcrest:hamcrest-library:1.3",
        "org.mockito:mockito-core:3.11.2",
        "org.mockito:mockito-inline:3.11.2",
        "org.slf4j:slf4j-api:" + SLF4J_VERSION,
        "org.slf4j:slf4j-simple:" + SLF4J_VERSION,
        "org.testcontainers:testcontainers:1.18.1",
        "org.testcontainers:localstack:1.18.1",
        "org.testcontainers:mockserver:1.18.1",
        "software.amazon.awssdk:apigateway:" + AWS_SDK_VERSION,
        "software.amazon.awssdk:arns:" + AWS_SDK_VERSION,
        "software.amazon.awssdk:autoscaling:" + AWS_SDK_VERSION,
        "software.amazon.awssdk:aws-sdk-java:" + AWS_SDK_VERSION,
        "software.amazon.awssdk:dynamodb-enhanced:" + AWS_SDK_VERSION,
        "software.amazon.awssdk:dynamodb:" + AWS_SDK_VERSION,
        "software.amazon.awssdk:cloudwatch:" + AWS_SDK_VERSION,
        "software.amazon.awssdk:ec2:" + AWS_SDK_VERSION,
        "software.amazon.awssdk:regions:" + AWS_SDK_VERSION,
        "software.amazon.awssdk:s3:" + AWS_SDK_VERSION,
        "software.amazon.awssdk:aws-core:" + AWS_SDK_VERSION,
        "software.amazon.awssdk:kms:" + AWS_SDK_VERSION,
        "software.amazon.awssdk:ssm:" + AWS_SDK_VERSION,
        "software.amazon.awssdk:sts:" + AWS_SDK_VERSION,
        "software.amazon.awssdk:sqs:" + AWS_SDK_VERSION,
        "software.amazon.awssdk:url-connection-client:" + AWS_SDK_VERSION,
        "software.amazon.awssdk:utils:" + AWS_SDK_VERSION,
        "software.amazon.awssdk:auth:" + AWS_SDK_VERSION,
        "software.amazon.awssdk:lambda:" + AWS_SDK_VERSION,
    ] + TINK_MAVEN_ARTIFACTS,
    repositories = [
        "https://repo1.maven.org/maven2",
    ],
)

################################################################################
# Download Maven Dependencies: End
################################################################################

############
# Go rules #
############
# Need to be after grpc_extra_deps to share go_register_toolchains.
load("@io_bazel_rules_go//go:deps.bzl", "go_rules_dependencies")
load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies")

go_rules_dependencies()

gazelle_dependencies()

###################
# Container rules #
###################
load(
    "@io_bazel_rules_docker//repositories:repositories.bzl",
    container_repositories = "repositories",
)

container_repositories()

load("@io_bazel_rules_docker//repositories:deps.bzl", container_deps = "deps")

container_deps()

load("@io_bazel_rules_docker//container:container.bzl", "container_pull")

#############
# PKG rules #
#############
load("@rules_pkg//:deps.bzl", "rules_pkg_dependencies")

rules_pkg_dependencies()

################################################################################
# Download Containers: Begin
################################################################################

# Base Images

# Base image for the MRP and some tests
container_pull(
    name = "java_base",
    digest = "sha256:d0ca593abaf2415c6828cad12c5cc8757d1ca7f50d544233ef442f85f9a9fae1",
    registry = "gcr.io",
    repository = "distroless/java17-debian12",
)

# Base layer of the lookup server base and builder images
container_pull(
    name = "debian12_base",
    digest = "sha256:1c62ac5d3b7b18ddbbda0f8a49f4d734a9ad21df68531f8f59f73babf28a9050",
    registry = "marketplace.gcr.io",
    repository = "google/debian12",
)

# Testing Images

# https://github.com/GoogleCloudPlatform/cloud-sdk-docker
container_pull(
   name = "pubsub_emulator",
   digest = "sha256:5f77874cdfa9c6b53ad13948af5b52dc7e96404ab9403af9f4005af150e584d4",
   registry = "gcr.io",
   repository = "google.com/cloudsdktool/google-cloud-cli",
   tag = "emulators",
)

# https://github.com/GoogleCloudPlatform/cloud-spanner-emulator
container_pull(
   name = "spanner_emulator",
   digest = "sha256:11b3615343c74d3c4ef7c4668305a87e2cab287dcab89fe2570e8d4d91938927",
   registry = "gcr.io",
   repository = "cloud-spanner-emulator/emulator",
   tag = "1.5.25",
)

# https://github.com/googleapis/storage-testbench
container_pull(
   name = "gcs_emulator",
   digest = "sha256:83e66a19feaa97c412747a071220a5a3b09c08432ce72d297b6974d9f0bed5e9",
   registry = "gcr.io",
   repository = "cloud-devrel-public-resources/storage-testbench",
   tag = "v0.49.0",
)

################################################################################
# Download Containers: End
################################################################################

################################################################################
# Download Apt Package Snapshots: Begin
################################################################################

# Creates Bazel repos for packages listed in yaml files
# https://github.com/GoogleContainerTools/rules_distroless/blob/v0.3.9/docs/apt_macro.md
load("@rules_distroless//apt:apt.bzl", "apt")

# Packages for the Lookup Server base image
# To update, follow instructions in the manifest file
apt.install(
    name = "lookup_server_apt",
    lock = "//cc/lookup_server/deploy:lookup_server_apt.lock.json",
    manifest = "//cc/lookup_server/deploy:lookup_server_apt.yaml",
)

load("@lookup_server_apt//:packages.bzl", "lookup_server_apt_packages")

lookup_server_apt_packages()

# Packages for the Lookup Server builder image
# To update, follow instructions in the manifest file
apt.install(
    name = "lookup_server_builder_apt",
    lock = "//cc/tools/build:lookup_server_builder_apt.lock.json",
    manifest = "//cc/tools/build:lookup_server_builder_apt.yaml",
)

load("@lookup_server_builder_apt//:packages.bzl", "lookup_server_builder_apt_packages")

lookup_server_builder_apt_packages()

################################################################################
# Download Apt Packages: End
################################################################################
