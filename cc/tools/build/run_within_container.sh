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


set -euox pipefail

if [[ "$*" != *"--bazel_command="* || "$*" != *"--bazel_output_directory="* ]]; then
  info_msg=$(cat <<-END
    Must provide all input variables. Switches are:\n
      --bazel_command=<value>\n
      --bazel_output_directory=<value>\n
END
)
  echo -e $info_msg
  exit 1
fi

is_louhi_release=false
while [ $# -gt 0 ]; do
  case "$1" in
    --bazel_command=*)
      bazel_command="${1#*=}"
      ;;
    --bazel_output_directory=*)
      bazel_output_directory="${1#*=}"
      ;;
    --output_tar_name=*)
      output_tar_name="${1#*=}"
      ;;
    --cp_source_path=*)
      cp_source_path="${1#*=}"
      ;;
    --cp_destination_path=*)
      cp_destination_path="${1#*=}"
      ;;
    --is_louhi_release=*)
      is_louhi_release="${1#*=}"
      ;;
    *)
      printf "***************************\n"
      printf "* Error: Invalid argument.*\n"
      printf "***************************\n"
      exit 1
  esac
  shift
done

if $is_louhi_release; then
  temp_storage_path="/workspace/louhi_ws/tmp/cfm-container-build"
  output_tar_path="/workspace/louhi_ws/tmp/cfm-tar"
else
  temp_storage_path="/tmp/cfm-container-build"
  output_tar_path="/tmp/cfm-tar"
fi

# Create the output directory if it does not exist
[ ! -d "$bazel_output_directory" ] && mkdir -p $bazel_output_directory

[[ $bazel_command != *"//cc"* ]] && \
  echo "" && \
  echo "ERROR: The path in the command [$bazel_command] does not start with the absolute //cc bazel target path." && \
  exit 1

repo_top_level_dir=$(git rev-parse --show-toplevel)
timestamp=$(date "+%Y%m%d-%H%M%S%N")
run_version="cfm_cc_build_$timestamp"
image_name='bazel/cc/tools/build:container_to_build_cc'

run_on_exit() {
  rm -rf $temp_storage_path

  echo ""
  if [ "$1" == "0" ]; then
    echo "Done :)"
  else
    echo "Done :("
  fi

  docker rm -f $(docker ps -a -q --format="{{.ID}}" \
    --filter ancestor="${image_name}")
}

# Make sure run_on_exit runs even when we encounter errors
trap "run_on_exit 1" ERR

# Set up the temporary working directories
rm -rf $temp_storage_path
mkdir -p $temp_storage_path

if [ -n "${output_tar_name-}" ]; then
  rm -rf $output_tar_path
  mkdir -p $output_tar_path
fi

# Determine the SCP tag or commit to use for the repository in the container
scp_tag=$(sed -rn 's/^SCP_VERSION\s*=\s*"(.*)"\s*(#.*)?$/\1/p' "${repo_top_level_dir}/WORKSPACE")
scp_commit=$(sed -rn 's/^SCP_COMMIT\s*=\s*"(.*)"\s*(#.*)?$/\1/p' "${repo_top_level_dir}/WORKSPACE")
scp_repo="$(grep -Phom1 '^\s*SCP_REPOSITORY\s*=\s*"\K[^"]*' "${repo_top_level_dir}/WORKSPACE")"
# Fetch the internal SCP repository (required as a dependency)
if [ -n "${scp_tag}" ] && [ -n "${scp_commit}" ]; then
  echo "Only one of SCP version tag or commit can be set in '${repo_top_level_dir}/WORKSPACE'."
  exit 1
elif [ -n "${scp_tag}" ]; then
  echo "Using SCP version tag: ${scp_tag}"
  (cd $temp_storage_path && git clone $scp_repo --depth 1 --branch $scp_tag scp)
elif [ -n "${scp_commit}" ]; then
  echo "Using SCP commit: ${scp_commit}"
  (cd $temp_storage_path && git clone $scp_repo scp && cd "scp" && git checkout "${scp_commit}")
else
  echo "Failed to read the SCP tag or commit from '${repo_top_level_dir}/WORKSPACE'."
  exit 1
fi

# Create and load the Lookup Service builder image as
# bazel/cc/tools/build:container_to_build_cc
bazel build //cc/tools/build:container_to_build_cc.tar
docker load --input "$(bazel info execution_root)/$(
  bazel cquery //cc/tools/build:container_to_build_cc.tar --output=files)"

docker_bazel_output_dir=$bazel_output_directory/$run_version

# Run the build container
# --privileged: Needed to access the docker socket on the host machine
# --network host: We want the tests to be executable the same way they would be on the host machine
docker -D run -d -i \
    --privileged \
    --network host \
    -v $docker_bazel_output_dir:/tmp/bazel_build_output \
    -v /var/run/docker.sock:/var/run/docker.sock \
    --name $run_version $image_name

# Copy the confidential match source code into the build container
docker cp $repo_top_level_dir $run_version:/cm

if [ -n "${cp_source_path:-}" ]; then
  echo "copying ${cp_source_path} to /cm/${cp_destination_path}"
  docker cp $cp_source_path $run_version:/cm/$cp_destination_path
fi

# Remove the bazel artifacts from the copied files if they exist
docker exec $run_version \
  bash -c "([[ $(find $repo_top_level_dir -name 'bazel-*' | wc -l) -gt 0 ]] && rm -rf /cm/bazel-*) || true"

# Update the confidential match dependencies in the workspace for a container build
docker exec $run_version \
  bash -c "sed -i 's/^#\s*CONTAINER_BUILD__UNCOMMENT://' /cm/WORKSPACE"
docker exec $run_version \
  bash -c "sed -i -n '/^#\s*CONTAINER_BUILD__REMOVE_SECTION\.START/,/^#\s*CONTAINER_BUILD__REMOVE_SECTION\.END/!p' /cm/WORKSPACE"

# Copy the SCP source code into the build container
docker cp "${temp_storage_path}/scp" $run_version:/scp

# Change the ownership of the copied files to the root user within the build container
docker exec $run_version chown -R root:root /cm
docker exec $run_version chown -R root:root /scp

# Set the build output directory
docker exec $run_version \
  bash -c "echo 'startup --output_user_root=/tmp/bazel_build_output' >> /cm/.bazelrc"

# Execute the command
docker exec -w /cm $run_version bash -c "$bazel_command"

# Change the build output directory permissions to the user running this script
user_id="$(id -u)"
docker exec $run_version chown -R $user_id:$user_id /tmp/bazel_build_output

if [ -n "${output_tar_name-}" ]; then
  # Copy the container image to the output
  cp $(find $docker_bazel_output_dir -name $output_tar_name) $output_tar_path
  cp $(find $docker_bazel_output_dir -name command.profile.gz) $output_tar_path || echo "Failed to find build profile."
fi
run_on_exit 0
