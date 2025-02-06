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

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def jemalloc():
    http_archive(
        name = "jemalloc",
        build_file = Label("//build_defs/cc/shared/build_targets:libjemalloc.BUILD"),
        sha256 = "1f35888bad9fd331f5a03445bc1bff808a59378be61fef01e9736179d76f2fab",
        url = "https://github.com/jemalloc/jemalloc/archive/refs/tags/5.3.0.zip",
    )
