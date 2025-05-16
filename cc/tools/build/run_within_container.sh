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

################################################################################
# Reproducibly build and load Lookup Service images and run C++ tests.
#
# Usage: run_within_container.sh [<bazel image tar target>|<bazel test targets>]
#
# If the first target ends in ".tar", the script will assume it is an image
# target, building it and then loading it into Docker. Otherwise, the targets
# will be run as Bazel tests. All targets are relative to the Bazel workspace
# directory.
#
# The container uses /tmp/cfm_build_output/ to store the Bazel output directory
# and generated image tar files.
#
# Prerequisites:
# * Bazel or Bazelisk is installed as "bazel"
# * gcloud is installed and can read Google Cloud ADC credentials (for an image
#   dependency)
# * Have read and write permissions to create and write to /tmp/cfm_build_output
# * Can execute Docker without sudo
################################################################################

set -euox pipefail

# Report failures and interruptions
trap "echo BUILD FAILED" ERR INT TERM

# Exit if no arguments were provided
if (( $# == 0 )); then
  echo ERROR: No arguments provided
  exit 1
fi

# Run in repo to guarantee that Bazel finds this workspace
cd "${BASH_SOURCE[0]%/*}/../../.."

# Load the builder image as bazel/cc/tools/build:container_to_build_cc
bazel run //cc/tools/build:container_to_build_cc

# Pull the SCP repository with Bazel, so it can be mounted to the container
bazel sync --only=com_google_adm_cloud_scp

# If $ROOT_DIR is set, this is in our Docker-in-Docker in automation
if [[ -n "${ROOT_DIR:+true}" ]]; then
  echo Detected execution in container
  # Create temp folder in workspace dir because it's in a volume
  # It won't be cached between runs since it can slow down automation
  trap "rm -rf ${PWD}/dind-tmp" EXIT
  mkdir -p "${PWD}/dind-tmp"
  out_dir="${PWD}/dind-tmp/cfm_build_output"
  # Move the SCP repo from the host output_base to the temp folder
  scp_dir="${PWD}/dind-tmp/scp"
  mv -T "$(bazel info output_base)/external/com_google_adm_cloud_scp" \
    "${scp_dir}"
else
  echo Detected execution on host OS
  out_dir=/tmp/cfm_build_output
  scp_dir="$(bazel info output_base)/external/com_google_adm_cloud_scp"
fi

# Create output folder using current user permissions
mkdir -p "${out_dir}"

# The first bash commands to run inside the container
# * Requires GCP credentials for Docker to pull the base image
# * Restores the user's permissions for the output directory after execution
container_init_command="
  echo startup --output_user_root=${out_dir}/bazel >> ~/.bazelrc
  echo common --override_repository=com_google_adm_cloud_scp=/scp >> ~/.bazelrc
  echo build --config=run-within-container >> ~/.bazelrc
  echo $(gcloud auth print-access-token) | docker login -u oauth2accesstoken \
    --password-stdin https://marketplace.gcr.io &> /dev/null
  trap 'chown --recursive --reference=${out_dir} ${out_dir} \
        && chmod --recursive --reference=${out_dir} ${out_dir}' EXIT
  chown --silent --recursive root:root ${out_dir}/bazel
  bazel sync --only=com_google_adm_cloud_scp
  "

# If there is one arg that ends in ".tar", build it and load it into Docker
# Otherwise, treat args as "bazel test" arguments
if [[ "$1" == *.tar ]]; then
  rm -f "${out_dir}/image.tar"
  bazel_command="bazel build $1 && cp -f \"\$(bazel info execution_root)/\$(
    bazel cquery --output=files $1)\" \"${out_dir}/image.tar\""
else
  bazel_command="bazel test $@"
fi

# Build in the container, with a read-only workspace and SCP repository
# * Uses a read-only volume for source code, rather than copying
# * Caches outputs across runs in /tmp/cfm_build_output when on a host OS
docker run --rm \
  --network host \
  -v "${out_dir}":"${out_dir}" \
  -v "${scp_dir}":/scp:ro \
  -v "$(bazel info workspace)":/src/confidential-match:ro \
  -w /src/confidential-match \
  bazel/cc/tools/build:container_to_build_cc \
  bash -c "${container_init_command} ${bazel_command}"

# If an image was built, load it
if [[ -f "${out_dir}/image.tar" ]]; then
  docker load --input "${out_dir}/image.tar"
fi

echo BUILD SUCCEEDED
