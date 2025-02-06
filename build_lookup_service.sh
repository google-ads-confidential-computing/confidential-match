#!/bin/bash
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

# Build the Lookup Service image tar
cc/tools/build/run_within_container.sh --bazel_command="bazel build \
  //cc/lookup_server/deploy:lookup_server_gcp_image_48_vcpu.tar \
  --//cc/lookup_server/server/src:platform=gcp \
  --@com_google_adm_cloud_scp//cc/public/cpio/interface:platform=gcp \
  --@com_google_adm_cloud_scp//cc/public/cpio/interface:run_inside_tee=True" \
  --bazel_output_directory=/tmp/cfm-tar \
  --output_tar_name=lookup_server_gcp_image_48_vcpu.tar

# Load the image into Docker as
# bazel/cc/lookup_server/deploy:lookup_server_gcp_image_48_vcpu
docker load --input /tmp/cfm-tar/lookup_server_gcp_image_48_vcpu.tar
