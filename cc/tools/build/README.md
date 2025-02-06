# Building Lookup Server

## Overview

`run_within_container.sh` builds the lookup server executable inside a
container.

Triggering a build pre-requisites:

1. This script must be run within this git repository
2. The path to the bazel target has to be absolute from the top of the
   repository
3. The output directory specified to put the build artifacts must be writable by
   the current user
4. Docker must be installed and available in the host machine
5. Docker must be executable sudoless
6. The user running the script must have credentials to fetch the SCP repository

## Example usage

```bash
$ cc/tools/build/run_within_container.sh --bazel_command="bazel build \
    //cc/lookup_server/server/src:lookup_server" \
    --bazel_output_directory=/tmp/my_output_dir
```

In the command above:

`--bazel_command` is the command to be executed. In this case it
is `bazel build`.<br>
Note that the path to the target is absolute form the top of the repository.
This is required.

`--bazel_output_directory` is a directory in the host machine running this
script, where the build artifacts will be placed.<br>
**This directory should ideally be under `/tmp`**<br>
The build will create a directory under the given `bazel_output_directory`
prefixed with `cm_cc_build_`

If the output directory is not important, this can be set to `$(mktemp -d)` to
just use a generic tmp directory.<br>
e.g. `--bazel_output_directory=$(mktemp -d)`

## Other examples

To build and run all the tests (except targets under `//cc/tools/build/`)
within the container:

```bash
$ cc/tools/build/run_within_container.sh \
    --bazel_command="bazel build //cc/... && bazel test //cc/..." \
    --bazel_output_directory="/tmp/my_output_dir"
```

To build staging container for gcp and output the image:

```bash
# Command to build the Lookup Service docker image (as tar file)
$ cc/tools/build/run_within_container.sh --bazel_command="bazel build \
  //cc/lookup_server/deploy:lookup_server_gcp_image_48_vcpu.tar \
  --//cc/lookup_server/server/src:platform=gcp \
  --@com_google_adm_cloud_scp//cc/public/cpio/interface:platform=gcp \
  --@com_google_adm_cloud_scp//cc/public/cpio/interface:run_inside_tee=True" \
  --bazel_output_directory=/tmp/cfm-tar \
  --output_tar_name=lookup_server_gcp_image_48_vcpu.tar
```
