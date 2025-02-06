# Confidential Match Repository

Confidential Match (CFM) allows matching between N sets of data on specific keys
while providing technical guarantees around confidentiality, integrity and
privacy.

## Build Instructions

### Prerequisites

Prior to building the CFM source make sure to have the following installed on
you Linux system.

* Debian Linux
  OS `NOTE: The following Debian version was used for verification.`

```
Debian, Debian GNU/Linux, 11 (bullseye), amd64 built on 20240910 (debian-11-bullseye-v20240910)
with Linux Kernel Debian 5.10.223-1 (2024-08-10) x86_64 GNU/Linux
```

* Docker Version 20.10.21\
  Reference: https://docs.docker.com/engine/install/debian/#install-using-the-repository \
  Non-root
  user: https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user

```bash
$ docker version
Client: Docker Engine - Community
 Version:           20.10.21
 API version:       1.41
 Go version:        go1.18.7
 Git commit:        baeda1f
 Built:             Tue Oct 25 18:02:28 2022
 OS/Arch:           linux/amd64
 Context:           default
 Experimental:      true

Server: Docker Engine - Community
 Engine:
  Version:          20.10.21
  API version:      1.41 (minimum version 1.12)
  Go version:       go1.18.7
  Git commit:       3056208
  Built:            Tue Oct 25 18:00:19 2022
  OS/Arch:          linux/amd64
  Experimental:     false
 containerd:
  Version:          1.7.22
  GitCommit:        7f7fdf5fed64eb6a7caf99b3e12efcf9d60e311c
 runc:
  Version:          1.1.14
  GitCommit:        v1.1.14-0-g2c9f560
 docker-init:
  Version:          0.19.0
  GitCommit:        de40ad0
```

* Bazel Version 6.3.0\
  Reference: https://docs.bazel.build/versions/0.19.1/install-ubuntu.html#installing-using-binary-installer

```bash
$ bazel version
Build label: 6.3.0
Build target: bazel-out/k8-opt/bin/src/main/java/com/google/devtools/build/lib/bazel/BazelServer_deploy.jar
Build time: Mon Jul 24 17:25:29 2023 (1690219529)
Build timestamp: 1690219529
Build timestamp as int: 1690219529
```

* Java Version OpenJDK 22.0.2\
  Reference: https://jdk.java.net/archive/

```bash
$ java -version
openjdk version "22.0.2" 2024-07-16
OpenJDK Runtime Environment (build 22.0.2+9-70)
OpenJDK 64-Bit Server VM (build 22.0.2+9-70, mixed mode, sharing)
```

* Python Version 3.8.2

```bash
$ python --version
Python 3.8.2
```

* Google Cloud CLI

```bash
$ gcloud --version
Google Cloud SDK 492.0.0
alpha 2024.09.06
beta 2024.09.06
bq 2.1.8
bundled-python3-unix 3.11.9
core 2024.09.06
gcloud-crc32c 1.0.0
gsutil 5.30
```

### Google Cloud Platform (GCP) Authentication

Before building the CFM source code, please be sure to authenticate your account
with GCP and config docker.

```bash
$ gcloud config set account <your-account>@gmail.com

$ gcloud auth login

$ gcloud auth configure-docker

$ gcloud auth configure-docker us-docker.pkg.dev
```

### Checkout Source Code via Git Tag

Prior to building the Lookup Service or the Match Request Processor, select the
desired git tag and then check out the source code corresponding to said git
tag.\
For example, the following commands (1) pull the latest code, (2) fetch the
latest tags and (3) checks out the code at a desired git tag of `v0.359.0`

```bash
$ git pull origin main --rebase
$ git fetch --tags
$ git checkout v0.359.0
```

### How to Build the Lookup Service

To build the Lookup Service, run the following command from the Git repository's
root directory.

```bash
$ ./build_lookup_service.sh
```

This will build the Lookup Service docker image and load it into docker with
following repository:tag name
`bazel/cc/lookup_server/deploy:lookup_server_gcp_image_48_vcpu`

To view the docker image ID run the following command:

```bash
$ docker images --no-trunc | grep lookup_server_gcp_image_48_vcpu
```

It produces output like the following:

```
bazel/cc/lookup_server/deploy lookup_server_gcp_image_48_vcpu sha256:2152f542fcf974d013371cc532db6cf5a9fb2b30ae18e6f9834e6d007df3ddb4 54 years ago 263MB
```

NOTE: The docker image ID in the above output
is `sha256:2152f542fcf974d013371cc532db6cf5a9fb2b30ae18e6f9834e6d007df3ddb4`

### How to Build the Match Request Processor (MRP)

To build the MRP, run the following command from the Git repository's root
directory.

```bash
$ ./build_mrp.sh
```

This will build the MRP docker image and load it into docker with following
repository:tag name
`bazel/java/com/google/cm/mrp:mrp_app_gcp_image`

To view the docker image ID run the following command:

```bash
$ docker images --no-trunc | grep mrp_app_gcp_image
```

It produces output like the following:

```
bazel/java/com/google/cm/mrp mrp_app_gcp_image sha256:181ac30961c1aa662d5901f790f218e7ea0ae595191133e015c15a9fc421f921	54 years ago	297MB
```

NOTE: The docker image ID in the above output
is `sha256:181ac30961c1aa662d5901f790f218e7ea0ae595191133e015c15a9fc421f921`

## How to Verify Image Signatures

Images are signed with [cosign](https://github.com/sigstore/cosign), which
stores signatures in a
[public registry](https://us-docker.pkg.dev/admcloud-cfm-public/docker-repo-signatures).
The steps recommended to verify a signature are as follows:

1. Install
   [cosign v1.11.1](https://github.com/sigstore/cosign/releases/tag/v1.11.1)
2. Build the image to be verified using steps from the sections above,
   either [MRP](#how-to-build-the-match-request-processor--mrp-)
   or [Lookup Service](#how-to-build-the-lookup-service)
3. Push the image to a repository using Docker, generating a digest that should
   match the released image digest
    * Note that pushing with a different tool can generate an incorrect digest
    * Choose a repository that you own that is not public
    * Here are example commands to push an MRP image built with the steps
      mentioned above, using Docker to push the image to the fake repository
      `us-docker.pkg.dev/cfm-verification-examples/mrp-example:v1.2.3`:

    ```bash
    $ docker tag bazel/java/com/google/cm/mrp:mrp_app_gcp_image us-docker.pkg.dev/cfm-verification-examples/mrp-example:v1.2.3
    $ docker push us-docker.pkg.dev/cfm-verification-examples/mrp-example:v1.2.3
    ```

4. Then run one of the following commands from this git repository root after
   replacing `registry/repository:tag` with the image that was pushed:
    * For a Match Request Processor image:

    ```bash
    $ COSIGN_REPOSITORY=us-docker.pkg.dev/admcloud-cfm-public/docker-repo-signatures/mrp_app_gcp_signature \
    cosign verify --key publickey.pem registry/repository:tag -a dev.cosignproject.cosign/sigalg=ECDSA_P256_SHA256 -a \
    dev.cosignproject.cosign/pub='LS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS0KTUZrd0V3WUhLb1pJemowQ0FRWUlLb1pJemowREFRY0RRZ0FFUm93NllsOVpyOFJyNWNvd1MvTDVTOHE4d1ROZQpYOUxaTUIxaXBhelFmN0pQNDFsYWthUHlCdDFKK3hyZUpKYW5RLy9wZExwczh6SUg1ZFBNTkFEdnN3PT0KLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tCg'
    ```

    * For a Lookup Service image:

    ```bash
    $ COSIGN_REPOSITORY=us-docker.pkg.dev/admcloud-cfm-public/docker-repo-signatures/lookup_server_gcp_signature \
    cosign verify --key publickey.pem registry/repository:tag -a dev.cosignproject.cosign/sigalg=ECDSA_P256_SHA256 -a \
    dev.cosignproject.cosign/pub='LS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS0KTUZrd0V3WUhLb1pJemowQ0FRWUlLb1pJemowREFRY0RRZ0FFUm93NllsOVpyOFJyNWNvd1MvTDVTOHE4d1ROZQpYOUxaTUIxaXBhelFmN0pQNDFsYWthUHlCdDFKK3hyZUpKYW5RLy9wZExwczh6SUg1ZFBNTkFEdnN3PT0KLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tCg'
    ```

    * This is an example command with a fake MRP image tagged
      `us-docker.pkg.dev/cfm-verification-examples/mrp-example:v1.2.3`
      which has the digest
      `sha:d719135212b741e3a1ccea71d6bd22351d0730c3b9a84967caf1e9a2f7f083b7`:

    ```bash
    $ COSIGN_REPOSITORY=us-docker.pkg.dev/admcloud-cfm-public/docker-repo-signatures/mrp_app_gcp_signature \
    cosign verify --key publickey.pem us-docker.pkg.dev/cfm-verification-examples/mrp-example:v1.2.3 -a dev.cosignproject.cosign/sigalg=ECDSA_P256_SHA256 -a \
    dev.cosignproject.cosign/pub='LS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS0KTUZrd0V3WUhLb1pJemowQ0FRWUlLb1pJemowREFRY0RRZ0FFUm93NllsOVpyOFJyNWNvd1MvTDVTOHE4d1ROZQpYOUxaTUIxaXBhelFmN0pQNDFsYWthUHlCdDFKK3hyZUpKYW5RLy9wZExwczh6SUg1ZFBNTkFEdnN3PT0KLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tCg'
    ```

   And the output should look similar to this:

    ````
    Verification for us-docker.pkg.dev/cfm-verification-examples/mrp-example:v1.2.3 --
    The following checks were performed on each of these signatures:
    - The specified annotations were verified.
    - The cosign claims were validated
    - The signatures were verified against the specified public key

    [{"critical":{"identity":{"docker-reference":"us-docker.pkg.dev/admcloud-cfm-services/docker-repo/mrp_app_gcp"},"image":{"docker-manifest-digest":"sha256:d719135212b741e3a1ccea71d6bd22351d0730c3b9a84967caf1e9a2f7f083b7"},"type":"cosign container image signature"},"optional":{"dev.cosignproject.cosign/pub":"LS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS0KTUZrd0V3WUhLb1pJemowQ0FRWUlLb1pJemowREFRY0RRZ0FFUm93NllsOVpyOFJyNWNvd1MvTDVTOHE4d1ROZQpYOUxaTUIxaXBhelFmN0pQNDFsYWthUHlCdDFKK3hyZUpKYW5RLy9wZExwczh6SUg1ZFBNTkFEdnN3PT0KLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tCg","dev.cosignproject.cosign/sigalg":"ECDSA_P256_SHA256"}}]
    ````
