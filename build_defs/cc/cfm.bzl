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

load("@com_google_adm_cloud_scp//build_defs/shared:absl.bzl", "absl")
load("@com_google_adm_cloud_scp//build_defs/shared:bazel_build_tools.bzl", "bazel_build_tools")
load("@com_google_adm_cloud_scp//build_defs/shared:bazel_rules_java.bzl", "bazel_rules_java")
load("@com_google_adm_cloud_scp//build_defs/shared:bazel_rules_pkg.bzl", "bazel_rules_pkg")
load("@com_google_adm_cloud_scp//build_defs/shared:bazel_docker_rules.bzl", "bazel_docker_rules")
load("@com_google_adm_cloud_scp//build_defs/shared:golang.bzl", "go_deps")
load("@com_google_adm_cloud_scp//build_defs/shared:java_grpc.bzl", "java_grpc")
load("@com_google_adm_cloud_scp//build_defs/shared:protobuf.bzl", "protobuf")
load("@com_google_adm_cloud_scp//build_defs/cc/shared:bazel_rules_cpp.bzl", "bazel_rules_cpp")
load("@com_google_adm_cloud_scp//build_defs/cc/shared:boost.bzl", "boost")
load("@com_google_adm_cloud_scp//build_defs/cc/shared:boringssl.bzl", "boringssl")
load("@com_google_adm_cloud_scp//build_defs/cc/shared:cc_utils.bzl", "cc_utils")
load("@com_google_adm_cloud_scp//build_defs/cc/shared:nghttp2.bzl", "nghttp2")
load("@com_google_adm_cloud_scp//build_defs/shared:google_cloud_sdk.bzl", "google_cloud_sdk")
load("@com_google_adm_cloud_scp//build_defs/shared:google_java_format.bzl", "google_java_format")
load("@com_google_adm_cloud_scp//build_defs/shared:rpm.bzl", "rpm")
load("@com_google_adm_cloud_scp//build_defs/shared:packer.bzl", "packer")
load("@com_google_adm_cloud_scp//build_defs/cc/shared:bazelisk.bzl", "bazelisk")
load("@com_google_adm_cloud_scp//build_defs/tink:tink_defs.bzl", "import_tink_git")

# Use `aws_sdk_cpp_static_lib_deps` when building inside container
load("@com_google_adm_cloud_scp//build_defs/cc/aws:aws_c_common_source_code.bzl", "aws_c_common")
load("@com_google_adm_cloud_scp//build_defs/cc/aws:aws_sdk_cpp_source_code_deps.bzl", "import_aws_sdk_cpp")
load("@com_google_adm_cloud_scp//build_defs/cc/shared:google_cloud_cpp.bzl", "import_google_cloud_cpp")

# CFM build defs
load("//build_defs/cc/shared:jemalloc.bzl", "jemalloc")

def cfm_dependencies(protobuf_version, protobuf_repo_hash):
    absl()
    bazel_build_tools()
    bazel_docker_rules()
    bazel_rules_cpp()
    bazel_rules_java()
    bazel_rules_pkg()
    bazelisk()
    boost()
    boringssl()
    cc_utils()
    go_deps()
    google_cloud_sdk()
    google_java_format()
    java_grpc()
    jemalloc()
    nghttp2()
    packer()
    protobuf(protobuf_version, protobuf_repo_hash)
    rpm()
    import_google_cloud_cpp()
    import_tink_git()

    aws_c_common()
    import_aws_sdk_cpp()
