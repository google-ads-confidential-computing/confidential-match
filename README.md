# Confidential Match

Confidential Match allows matching between two sets of data on specific keys
while providing technical guarantees around confidentiality, integrity, and
privacy.

The following two components run inside a Trusted Execution Environment (TEE),
which provides a technical guarantee about how the data is processed.

1. Lookup Service
2. Match Request Processor (MRP)

## Build Instructions

### Prerequisites

Confidential Match source code and images are built with the following
dependencies:

* Debian 12 (Bookworm) on x86_64
* Docker Engine 20.10.24
    * https://docs.docker.com/engine/install/debian/#install-using-the-repository
    * https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user
* Bazel 6.4.0
    * https://bazel.build/versions/6.4.0/install/ubuntu
    * If installing Bazelisk, it must be callable with the command `bazel`
* OpenJDK JDK 17
* Python 3.12.8
    * Installed using https://github.com/pyenv/pyenv
* Go (golang) 1.19.8
* Google Cloud SDK (gcloud) 530.0.0
    * https://cloud.google.com/sdk/docs/install

### Google Cloud Platform (GCP) Authentication

These commands will authenticate your account using gcloud and configure its
Docker credential helper.

```bash
$ gcloud config set account <your-account>@gmail.com

$ gcloud auth login

$ gcloud auth configure-docker

$ gcloud auth configure-docker us-docker.pkg.dev
```

### How to Build the Lookup Service

Run `build_lookup_service.sh` to build the image and load it into Docker
as `bazel/cc/lookup_server/deploy:lookup_server_gcp_image_48_vcpu`.

### How to Run Lookup Service Tests

Run `bazel test //cc/...` to execute these tests.

### How to Build the Match Request Processor (MRP)

Run `build_mrp.sh` to build the image and load it into Docker
as `bazel/java/com/google/cm/mrp:mrp_app_gcp_image`.

### How to Run Match Request Processor Tests

Run `bazel test //javatests/...` to execute these tests.

## How to Verify Image Signatures

Deployed images are signed with [cosign](https://github.com/sigstore/cosign),
and the signatures are stored in a
[public repository](https://us-docker.pkg.dev/admcloud-cfm-public/docker-repo-signatures).
Use the following steps to verify that the source code produces an image that
matches a public signature:

### Verify the Match Request Processor Image Signature

1. Install
   [cosign v1.11.1](https://github.com/sigstore/cosign/releases/tag/v1.11.1)
2. [Build the MRP image](#how-to-build-the-match-request-processor-mrp)
3. Push the image to a private repository using Docker
    * Choose a repository that you own that is not public
    * The remote image should have a digest matching the released image's digest
    * Example commands:

    ```bash
    $ MRP_REPOSITORY=<your-private-mrp-repository>
    $ TAG=<your-chosen-image-tag>
    $ docker tag bazel/java/com/google/cm/mrp:mrp_app_gcp_image "${MRP_REPOSITORY}:${TAG}"
    $ docker push "${MRP_REPOSITORY}:${TAG}"
    ```

4. Run these commands from this Git repository's root directory:

    ```bash
    $ MRP_REPOSITORY=<your-private-mrp-repository>
    $ TAG=<your-chosen-image-tag>
    $ COSIGN_REPOSITORY=us-docker.pkg.dev/admcloud-cfm-public/docker-repo-signatures/mrp_app_gcp_signature \
    cosign verify --key publickey.pem "${MRP_REPOSITORY}:${TAG}" -a dev.cosignproject.cosign/sigalg=ECDSA_P256_SHA256 -a \
    dev.cosignproject.cosign/pub='LS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS0KTUZrd0V3WUhLb1pJemowQ0FRWUlLb1pJemowREFRY0RRZ0FFUm93NllsOVpyOFJyNWNvd1MvTDVTOHE4d1ROZQpYOUxaTUIxaXBhelFmN0pQNDFsYWthUHlCdDFKK3hyZUpKYW5RLy9wZExwczh6SUg1ZFBNTkFEdnN3PT0KLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tCg'
    ```

   The output should look similar to this:

    ```
    Verification for us-docker.pkg.dev/cfm-verification-examples/mrp-example:v1.2.3 --
    The following checks were performed on each of these signatures:
    - The specified annotations were verified.
    - The cosign claims were validated
    - The signatures were verified against the specified public key

    [{"critical":{"identity":{"docker-reference":"us-docker.pkg.dev/admcloud-cfm-services/docker-repo/mrp_app_gcp"},"image":{"docker-manifest-digest":"sha256:d719135212b741e3a1ccea71d6bd22351d0730c3b9a84967caf1e9a2f7f083b7"},"type":"cosign container image signature"},"optional":{"dev.cosignproject.cosign/pub":"LS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS0KTUZrd0V3WUhLb1pJemowQ0FRWUlLb1pJemowREFRY0RRZ0FFUm93NllsOVpyOFJyNWNvd1MvTDVTOHE4d1ROZQpYOUxaTUIxaXBhelFmN0pQNDFsYWthUHlCdDFKK3hyZUpKYW5RLy9wZExwczh6SUg1ZFBNTkFEdnN3PT0KLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tCg","dev.cosignproject.cosign/sigalg":"ECDSA_P256_SHA256"}}]
    ```

### Verify the Lookup Service Image Signature

1. Install
   [cosign v1.11.1](https://github.com/sigstore/cosign/releases/tag/v1.11.1)
2. [Build the Lookup Service image](#how-to-build-the-lookup-service)
3. Push the image to a private repository using Docker
    * Choose a repository that you own that is not public
    * The remote image should have a digest matching the released image's digest
    * Example commands:

    ```bash
    $ LS_REPOSITORY=<your-private-lookup-server-repository>
    $ TAG=<your-chosen-image-tag>
    $ docker tag bazel/cc/lookup_server/deploy:lookup_server_gcp_image_48_vcpu "${LS_REPOSITORY}:${TAG}"
    $ docker push "${LS_REPOSITORY}:${TAG}"
    ```

4. Run these commands from this Git repository's root directory:

    ```bash
    $ LS_REPOSITORY=<your-private-lookup-server-repository>
    $ TAG=<your-chosen-image-tag>
    $ COSIGN_REPOSITORY=us-docker.pkg.dev/admcloud-cfm-public/docker-repo-signatures/lookup_server_gcp_signature \
    cosign verify --key publickey.pem "${LS_REPOSITORY}:${TAG}" -a dev.cosignproject.cosign/sigalg=ECDSA_P256_SHA256 -a \
    dev.cosignproject.cosign/pub='LS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS0KTUZrd0V3WUhLb1pJemowQ0FRWUlLb1pJemowREFRY0RRZ0FFUm93NllsOVpyOFJyNWNvd1MvTDVTOHE4d1ROZQpYOUxaTUIxaXBhelFmN0pQNDFsYWthUHlCdDFKK3hyZUpKYW5RLy9wZExwczh6SUg1ZFBNTkFEdnN3PT0KLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tCg'
    ```

   The output should look similar to this:

    ```
    Verification for us-docker.pkg.dev/cfm-verification-examples/ls-example:v1.2.3 --
    The following checks were performed on each of these signatures:
    - The specified annotations were verified.
    - The cosign claims were validated
    - The signatures were verified against the specified public key

    [{"critical":{"identity":{"docker-reference":"us-docker.pkg.dev/admcloud-cfm-data/docker-repo-dev/lookup_server_gcp"},"image":{"docker-manifest-digest":"sha256:d719135212b741e3a1ccea71d6bd22351d0730c3b9a84967caf1e9a2f7f083b7"},"type":"cosign container image signature"},"optional":{"dev.cosignproject.cosign/pub":"LS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS0KTUZrd0V3WUhLb1pJemowQ0FRWUlLb1pJemowREFRY0RRZ0FFUm93NllsOVpyOFJyNWNvd1MvTDVTOHE4d1ROZQpYOUxaTUIxaXBhelFmN0pQNDFsYWthUHlCdDFKK3hyZUpKYW5RLy9wZExwczh6SUg1ZFBNTkFEdnN3PT0KLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tCg","dev.cosignproject.cosign/sigalg":"ECDSA_P256_SHA256"}}]
    ```
