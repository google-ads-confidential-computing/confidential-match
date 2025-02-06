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

# TODO(b/379357774): With rules_oci, we can safely load from an oci_load target

# Build the MRP image tar
bazel build //java/com/google/cm/mrp:mrp_app_gcp_image.tar

# Load the image into Docker as bazel/java/com/google/cm/mrp:mrp_app_gcp_image
docker load --input "$(bazel info execution_root)/$(
  bazel cquery //java/com/google/cm/mrp:mrp_app_gcp_image.tar --output=files)"
