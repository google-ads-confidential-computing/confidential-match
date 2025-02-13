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
load("@rules_distroless//apt:defs.bzl", "dpkg_statusd")
load("@rules_distroless//distroless:defs.bzl", "cacerts")
load("@rules_pkg//:pkg.bzl", "pkg_tar")

# Packages added to the lookup server image, including dependencies
# When packages change, update this list with this command's output:
#   jq '.packages[].name' lookup_server_apt.lock.json
LS_PACKAGE_NAMES = [
    "ca-certificates",
    "debconf",
    "perl-base",
    "dpkg",
    "tar",
    "libselinux1",
    "libpcre2-8-0",
    "libc6",
    "libcrypt1",
    "libgcc-s1",
    "gcc-10-base",
    "libacl1",
    "zlib1g",
    "liblzma5",
    "libbz2-1.0",
    "openssl",
    "libssl1.1",
    "libatomic1",
    "libxml2-dev",
    "libxml2",
    "libicu67",
    "libstdc++6",
    "libicu-dev",
    "libc6-dev",
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
    "linux-libc-dev",
    "libc-dev-bin",
    "icu-devtools",
    "netbase",
    "rsyslog",
    "libuuid1",
    "libsystemd0",
    "libzstd1",
    "liblz4-1",
    "libgcrypt20",
    "libgpg-error0",
    "liblognorm5",
    "libfastjson4",
    "libestr0",
    "init-system-helpers",
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

# Lookup server executable
pkg_tar(
    name = "lookup_service_layer",
    srcs = ["//cc/lookup_server/server/src:lookup_server"],
    include_runfiles = True,
    mode = "0755",
    ownername = "root.root",
    package_dir = "opt/google/lookup_service",
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

container_image(
    name = "lookup_server_gcp_image_4_vcpu",
    base = "@debian_11//image",
    cmd = [
        "/bin/bash",
        "startup_script",
    ],
    env = {
        "enabled_log_levels": "Debug,Info,Warning,Error,Critical",
        "async_executor_queue_size": "1000",
        "async_executor_threads_count": "4",  # For n2d-standard-4
        "io_async_executor_queue_size": "1000",
        "io_async_executor_threads_count": "50",
        # Required to use :cacerts, see that target definition for details
        "SSL_CERT_FILE": "/etc/ssl/certs/ca-certificates.crt",
    },
    files = ["startup_script"],
    labels = {
        "tee.launch_policy.allow_cmd_override": "false",
        "tee.launch_policy.allow_env_override": "cluster_id,cluster_group_id,environment",
        "tee.launch_policy.log_redirect": "always",
        "tee.launch_policy.monitoring_memory_allow": "always",
    },
    ports = [
        "8080/tcp",
        "8081/tcp",
    ],
    tags = ["manual"],
    tars = [
        ":cacerts",
        ":lookup_service_layer",
    ] + LS_PACKAGES + LS_PACKAGE_INSTALL_STATUSES,
    volumes = ["/opt/google/ssl/self-signed-certs"],
)

container_image(
    name = "lookup_server_gcp_image_48_vcpu",
    base = "@debian_11//image",
    cmd = [
        "/bin/bash",
        "startup_script",
    ],
    env = {
        "enabled_log_levels": "Info,Warning,Error,Critical",
        "async_executor_queue_size": "100000",
        "async_executor_threads_count": "48",  # For n2d-highmem-48
        "io_async_executor_queue_size": "100000",
        "io_async_executor_threads_count": "500",
        # Required to use :cacerts, see that target definition for details
        "SSL_CERT_FILE": "/etc/ssl/certs/ca-certificates.crt",
    },
    files = ["startup_script"],
    labels = {
        "tee.launch_policy.allow_cmd_override": "false",
        "tee.launch_policy.allow_env_override": "cluster_id,cluster_group_id,environment",
        "tee.launch_policy.log_redirect": "always",
        "tee.launch_policy.monitoring_memory_allow": "always",
    },
    ports = [
        "8080/tcp",
        "8081/tcp",
    ],
    tags = ["manual"],
    tars = [
        ":cacerts",
        ":lookup_service_layer",
    ] + LS_PACKAGES + LS_PACKAGE_INSTALL_STATUSES,
    volumes = ["/opt/google/ssl/self-signed-certs"],
)
