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

load("@rules_distroless//apt:defs.bzl", "dpkg_statusd")
load("@rules_distroless//distroless:defs.bzl", "cacerts")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_load")
load("@rules_pkg//pkg:mappings.bzl", "pkg_attributes", "pkg_files")
load("@rules_pkg//:pkg.bzl", "pkg_tar")

package(default_visibility = ["//visibility:public"])

# Packages added to the lookup server builder image, including dependencies.
# See lookup_server_builder_apt.yaml for more details.
LS_BUILDER_PACKAGE_NAMES = [
    "autoconf",
    "debianutils",
    "libc6",
    "libgcc-s1",
    "gcc-12-base",
    "m4",
    "perl",
    "libperl5.36",
    "perl-modules-5.36",
    "perl-base",
    "dpkg",
    "tar",
    "libselinux1",
    "libpcre2-8-0",
    "libacl1",
    "zlib1g",
    "libzstd1",
    "libmd0",
    "liblzma5",
    "libbz2-1.0",
    "libcrypt1",
    "libgdbm6",
    "libgdbm-compat4",
    "libdb5.3",
    "build-essential",
    "dpkg-dev",
    "binutils",
    "binutils-x86-64-linux-gnu",
    "libstdc++6",
    "libjansson4",
    "libgprofng0",
    "libbinutils",
    "binutils-common",
    "libctf0",
    "libctf-nobfd0",
    "make",
    "patch",
    "xz-utils",
    "bzip2",
    "libdpkg-perl",
    "g++",
    "gcc-12",
    "libmpfr6",
    "libgmp10",
    "libmpc3",
    "libisl23",
    "libgcc-12-dev",
    "libquadmath0",
    "libubsan1",
    "libtsan2",
    "liblsan0",
    "libasan8",
    "libatomic1",
    "libitm1",
    "libgomp1",
    "libcc1-0",
    "cpp-12",
    "g++-12",
    "libstdc++-12-dev",
    "libc6-dev",
    "rpcsvc-proto",
    "libnsl-dev",
    "libtirpc-dev",
    "libtirpc3",
    "libtirpc-common",
    "libgssapi-krb5-2",
    "libkrb5support0",
    "libkrb5-3",
    "libssl3",
    "libkeyutils1",
    "libk5crypto3",
    "libcom-err2",
    "libnsl2",
    "libcrypt-dev",
    "linux-libc-dev",
    "libc-dev-bin",
    "gcc",
    "cpp",
    "ca-certificates",
    "debconf",
    "openssl",
    "clang-13",
    "libclang1-13",
    "libllvm13",
    "libz3-4",
    "libxml2",
    "libicu72",
    "libtinfo6",
    "libffi8",
    "libedit2",
    "libbsd0",
    "llvm-13-linker-tools",
    "libclang-common-13-dev",
    "libc6-i386",
    "lib32stdc++6",
    "lib32gcc-s1",
    "libobjc-12-dev",
    "libobjc4",
    "libgc1",
    "libclang-cpp13",
    "cmake",
    "procps",
    "init-system-helpers",
    "usr-is-merged",
    "libproc2-0",
    "libsystemd0",
    "liblz4-1",
    "libgcrypt20",
    "libgpg-error0",
    "libcap2",
    "libncursesw6",
    "cmake-data",
    "libuv1",
    "librhash0",
    "libjsoncpp25",
    "libexpat1",
    "libcurl4",
    "libssh2-1",
    "librtmp1",
    "libnettle8",
    "libhogweed6",
    "libgnutls30",
    "libunistring2",
    "libtasn1-6",
    "libp11-kit0",
    "libidn2-0",
    "libpsl5",
    "libnghttp2-14",
    "libldap-2.5-0",
    "libsasl2-2",
    "libsasl2-modules-db",
    "libbrotli1",
    "libarchive13",
    "curl",
    "docker.io",
    "tini",
    "runc",
    "libseccomp2",
    "lsb-base",
    "sysvinit-utils",
    "iptables",
    "libnftnl11",
    "libmnl0",
    "libnfnetlink0",
    "libnetfilter-conntrack3",
    "netbase",
    "libxtables12",
    "libip6tc2",
    "libip4tc2",
    "containerd",
    "adduser",
    "passwd",
    "libpam-modules",
    "libpam-modules-bin",
    "libpam0g",
    "libaudit1",
    "libcap-ng0",
    "libaudit-common",
    "libsemanage2",
    "libsepol2",
    "libsemanage-common",
    "libdevmapper1.02.1",
    "dmsetup",
    "libudev1",
    "git",
    "git-man",
    "liberror-perl",
    "libcurl3-gnutls",
    "golang",
    "golang-src",
    "golang-1.19-src",
    "golang-go",
    "golang-1.19-go",
    "golang-doc",
    "golang-1.19-doc",
    "golang-1.19",
    "libcurl4-openssl-dev",
    "openjdk-17-jre",
    "libxtst6",
    "x11-common",
    "libxext6",
    "libx11-6",
    "libx11-data",
    "libxcb1",
    "libxdmcp6",
    "libxau6",
    "libxrender1",
    "libxi6",
    "libpng16-16",
    "libjpeg62-turbo",
    "libgif7",
    "libharfbuzz0b",
    "libgraphite2-3",
    "libglib2.0-0",
    "libmount1",
    "libblkid1",
    "libfreetype6",
    "libgl1",
    "libglx0",
    "libglx-mesa0",
    "libgl1-mesa-dri",
    "libxcb-dri3-0",
    "libsensors5",
    "libsensors-config",
    "libllvm15",
    "libglapi-mesa",
    "libelf1",
    "libdrm2",
    "libdrm-common",
    "libdrm-radeon1",
    "libdrm-nouveau2",
    "libdrm-intel1",
    "libpciaccess0",
    "libdrm-amdgpu1",
    "libxxf86vm1",
    "libxshmfence1",
    "libxfixes3",
    "libxcb-xfixes0",
    "libxcb-sync1",
    "libxcb-shm0",
    "libxcb-randr0",
    "libxcb-present0",
    "libxcb-glx0",
    "libxcb-dri2-0",
    "libx11-xcb1",
    "libglvnd0",
    "libxinerama1",
    "libxrandr2",
    "libgtk-3-0",
    "libgtk-3-common",
    "dconf-gsettings-backend",
    "libdconf1",
    "dconf-service",
    "libxkbcommon0",
    "xkb-data",
    "libxdamage1",
    "libxcursor1",
    "libxcomposite1",
    "libwayland-egl1",
    "libwayland-cursor0",
    "libwayland-client0",
    "libpangoft2-1.0-0",
    "libpango-1.0-0",
    "libthai0",
    "libdatrie1",
    "libthai-data",
    "libfribidi0",
    "fontconfig",
    "fontconfig-config",
    "fonts-texgyre",
    "libfontconfig1",
    "libpangocairo-1.0-0",
    "libcairo2",
    "libxcb-render0",
    "libpixman-1-0",
    "libgdk-pixbuf-2.0-0",
    "libtiff6",
    "libwebp7",
    "liblerc4",
    "libjbig0",
    "libdeflate0",
    "shared-mime-info",
    "libgdk-pixbuf2.0-common",
    "libepoxy0",
    "libcups2",
    "libavahi-common3",
    "libavahi-common-data",
    "libavahi-client3",
    "libdbus-1-3",
    "libcolord2",
    "liblcms2-2",
    "libcairo-gobject2",
    "libatk1.0-0",
    "at-spi2-common",
    "libatk-bridge2.0-0",
    "libatspi2.0-0",
    "hicolor-icon-theme",
    "adwaita-icon-theme",
    "gtk-update-icon-cache",
    "openjdk-17-jre-headless",
    "libpcsclite1",
    "libasound2",
    "libasound2-data",
    "util-linux",
    "util-linux-extra",
    "libsmartcols1",
    "libuuid1",
    "libnss3",
    "libsqlite3-0",
    "libnspr4",
    "java-common",
    "ca-certificates-java",
    "protobuf-compiler",
    "libprotoc32",
    "libprotobuf32",
    "python-is-python3",
    "python3",
    "libpython3-stdlib",
    "libpython3.11-stdlib",
    "libreadline8",
    "readline-common",
    "install-info",
    "mime-support",
    "media-types",
    "mailcap",
    "libpython3.11-minimal",
    "python3.11",
    "python3.11-minimal",
    "python3-minimal",
    "rsyslog",
    "liblognorm5",
    "libfastjson4",
    "libestr0",
    "socat",
    "libwrap0",
    "zlib1g-dev",
]

# Package installer build targets
LS_BUILDER_PACKAGES = [
    "@lookup_server_builder_apt//" + pkg + "/amd64"
    for pkg in LS_BUILDER_PACKAGE_NAMES
]

# Include package installation metadata
[
    dpkg_statusd(
        name = "dpkg_statusd_" + pkg,
        package_name = pkg,
        control = "@lookup_server_builder_apt//" + pkg + "/amd64:control",
        tags = ["manual"],
    )
    for pkg in LS_BUILDER_PACKAGE_NAMES
]

# Package installation status build targets
LS_BUILDER_PACKAGE_INSTALL_STATUSES = [
    "dpkg_statusd_" + pkg
    for pkg in LS_BUILDER_PACKAGE_NAMES
]

# Bundles ca-certificates as /etc/ssl/certs/ca-certificates.crt
# Images must set the environment variable SSL_CERT_FILE to this full file path
# https://github.com/GoogleContainerTools/rules_distroless/blob/v0.3.9/docs/rules.md#cacerts
cacerts(
    name = "cacerts",
    package = "@lookup_server_builder_apt//ca-certificates/amd64:data",
    tags = ["manual"],
)

# Initial tar used for following steps that fix apt dep installation directories
# Workaround for https://github.com/GoogleContainerTools/rules_distroless/issues/53
# For details, see https://www.debian.org/releases/bookworm/amd64/release-notes/ch-information.en.html#a-merged-usr-is-now-required
pkg_tar(
    name = "unfixed_apt_deps",
    tags = ["manual"],
    deps = LS_BUILDER_PACKAGES + LS_BUILDER_PACKAGE_INSTALL_STATUSES + [":cacerts"],
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

pkg_files(
    name = "bazelisk_file",
    srcs = ["@bazelisk//file"],
    attributes = pkg_attributes(mode = "0755"),
    renames = {"@bazelisk//file": "bazel"},
    tags = ["manual"],
)

pkg_tar(
    name = "bazelisk_layer",
    srcs = [":bazelisk_file"],
    package_dir = "/usr/local/bin/",
    tags = ["manual"],
)

oci_image(
    name = "container_to_build_cc",
    base = "@debian12_base",
    env = {
        # Required to use :cacerts, see that target definition for details
        "SSL_CERT_FILE": "/etc/ssl/certs/ca-certificates.crt",
        "CC": "/usr/bin/clang-13",
        "CXX": "/usr/bin/clang++-13",
    },
    tags = ["manual"],
    tars = [
        ":bazelisk_layer",
        ":apt_deps_layer",
    ],
)

oci_load(
    name = "container_to_build_cc_load",
    image = ":container_to_build_cc",
    repo_tags = ["bazel/cc/tools/build:container_to_build_cc"],
    tags = ["manual"],
)

# Legacy target for building tar files directly
filegroup(
    name = "container_to_build_cc.tar",
    srcs = [":container_to_build_cc_load"],
    output_group = "tarball",
    tags = ["manual"],
)
