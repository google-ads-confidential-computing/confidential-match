#!/usr/bin/env bash
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

# Build the Lookup Service image and load it as
# bazel/cc/lookup_server/deploy:lookup_server_gcp_image_48_vcpu
"${BASH_SOURCE[0]%/*}/cc/tools/build/run_within_container.sh" \
  //cc/lookup_server/deploy:lookup_server_gcp_image_48_vcpu.tar
