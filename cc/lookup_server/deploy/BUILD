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

load("@aspect_bazel_lib//lib:tar.bzl", "tar")
load("@rules_distroless//apt:defs.bzl", "dpkg_statusd")
load("@rules_distroless//distroless:defs.bzl", "cacerts")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_load")
load("@rules_pkg//:pkg.bzl", "pkg_tar")

# Packages added to the lookup server image, including dependencies.
# See lookup_server_apt.yaml for more details.
LS_PACKAGE_NAMES = [
    "ca-certificates",
    "debconf",
    "openssl",
    "libssl3",
    "libc6",
    "libgcc-s1",
    "gcc-12-base",
    "libatomic1",
    "libxml2-dev",
    "libxml2",
    "zlib1g",
    "liblzma5",
    "libicu72",
    "libstdc++6",
    "libicu-dev",
    "libc6-dev",
    "rpcsvc-proto",
    "libnsl-dev",
    "libtirpc-dev",
    "libtirpc3",
    "libtirpc-common",
    "libgssapi-krb5-2",
    "libkrb5support0",
    "libkrb5-3",
    "libkeyutils1",
    "libk5crypto3",
    "libcom-err2",
    "libnsl2",
    "libcrypt-dev",
    "libcrypt1",
    "linux-libc-dev",
    "libc-dev-bin",
    "icu-devtools",
    "netbase",
    "rsyslog",
    "libzstd1",
    "libuuid1",
    "libsystemd0",
    "liblz4-1",
    "libgcrypt20",
    "libgpg-error0",
    "libcap2",
    "liblognorm5",
    "libfastjson4",
    "libestr0",
]

# Package installer build targets
LS_PACKAGES = [
    "@lookup_server_apt//" + pkg + "/amd64"
    for pkg in LS_PACKAGE_NAMES
]

# Include package installation metadata
[
    dpkg_statusd(
        name = "dpkg_statusd_" + pkg,
        package_name = pkg,
        control = "@lookup_server_apt//" + pkg + "/amd64:control",
        tags = ["manual"],
    )
    for pkg in LS_PACKAGE_NAMES
]

# Package installation status build targets
LS_PACKAGE_INSTALL_STATUSES = [
    "dpkg_statusd_" + pkg
    for pkg in LS_PACKAGE_NAMES
]

# A unique dir for the flattened layer, so that Docker only uses our version
tar(
    name = "empty_dir",
    mtree = ["./confidential-match-ls-debian12_base uid=0 gid=0 time=1672560000 mode=0755 type=dir"],
    tags = ["manual"],
)

oci_image(
    name = "base_image",
    base = "@debian12_base",
    tags = ["manual"],
    tars = [":empty_dir"],
)

oci_load(
    name = "base_image_load",
    image = ":base_image",
    repo_tags = [":base_image"],
    tags = ["manual"],
)

filegroup(
    name = "base_image_load.tar",
    srcs = [":base_image_load"],
    output_group = "tarball",
    tags = ["manual"],
)

filegroup(
    name = "crane",
    srcs = ["@oci_crane_toolchains//:current_toolchain"],
    tags = ["manual"],
)

genrule(
    name = "base_layer",
    srcs = [":base_image_load.tar"],
    outs = ["base_layer_flattened.tar"],
    cmd = "./$(location crane) export - $@ < $<",
    tags = ["manual"],
    tools = [":crane"],
)

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
# https://github.com/GoogleContainerTools/rules_distroless/blob/v0.3.9/docs/rules.md#cacerts
cacerts(
    name = "cacerts",
    package = "@lookup_server_apt//ca-certificates/amd64:data",
    tags = ["manual"],
)

# Initial tar used for following steps that fix apt dep installation directories
# Workaround for https://github.com/GoogleContainerTools/rules_distroless/issues/53
# For details, see https://www.debian.org/releases/bookworm/amd64/release-notes/ch-information.en.html#a-merged-usr-is-now-required
pkg_tar(
    name = "unfixed_apt_deps",
    tags = ["manual"],
    deps = LS_PACKAGES + LS_PACKAGE_INSTALL_STATUSES + [":cacerts"],
)

# Creates a tar file that only keeps directories already in correct locations
genrule(
    name = "correct_dirs_apt_deps",
    srcs = [":unfixed_apt_deps"],
    outs = ["correct_dirs_apt_deps.tar"],
    cmd = """
    cp $< $@
    chmod +w $@
    for DIR in './bin' './sbin' './lib*'; do
      if (($$(tar -tf $< --wildcards $$DIR &>/dev/null ; echo $$?)==0)); then
        tar -f $@ --wildcards --delete $$DIR
      fi
    done
    """,
    tags = ["manual"],
)

# Creates a tar file that only keeps directories in wrong locations
# i.e. /bin, /sbin, /lib*
genrule(
    name = "wrong_dirs_apt_deps",
    srcs = [":unfixed_apt_deps"],
    outs = ["wrong_dirs_apt_deps.tar"],
    cmd = """
    cp $< $@
    chmod +w $@
    tar -tf $< --wildcards --exclude './*/*' \
      --exclude './bin' --exclude './sbin' --exclude './lib*' | \
      tar -f $@ --delete -T -
    """,
    tags = ["manual"],
)

# Creates a tar file that fixes the incorrect directories
pkg_tar(
    name = "remapped_dirs_apt_deps",
    package_dir = "./usr",
    tags = ["manual"],
    deps = [":wrong_dirs_apt_deps"],
)

# Merges apt dep tar files with correct directories
pkg_tar(
    name = "apt_deps_layer",
    package_dir = ".",
    tags = ["manual"],
    deps = [
        ":correct_dirs_apt_deps",
        ":remapped_dirs_apt_deps",
    ],
)

# Restores base image config values that were removed during flattening
oci_image(
    name = "lookup_server_gcp_image_4_vcpu",
    architecture = "amd64",
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
    os = "linux",
    tags = ["manual"],
    # Tar layers are ordered from least-to-most likely to change between builds
    tars = [
        ":base_layer",
        ":startup_script_layer",
        ":apt_deps_layer",
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

# Legacy target for building tar files directly
filegroup(
    name = "lookup_server_gcp_image_4_vcpu.tar",
    srcs = [":lookup_server_gcp_image_4_vcpu_load"],
    output_group = "tarball",
    tags = ["manual"],
)

# Restores base image config values that were removed during flattening
oci_image(
    name = "lookup_server_gcp_image_48_vcpu",
    architecture = "amd64",
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
    os = "linux",
    tags = ["manual"],
    # Tar layers are ordered from least-to-most likely to change between builds
    tars = [
        ":base_layer",
        ":startup_script_layer",
        ":apt_deps_layer",
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

# Legacy target for building tar files directly
filegroup(
    name = "lookup_server_gcp_image_48_vcpu.tar",
    srcs = [":lookup_server_gcp_image_48_vcpu_load"],
    output_group = "tarball",
    tags = ["manual"],
)
