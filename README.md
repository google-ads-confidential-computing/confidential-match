# Confidential Match Repository

Confidential Match (CFM) allows matching between N sets of data on specific keys
while providing technical guarantees around confidentiality, integrity and
privacy.

## Build Instructions

### Prerequisites

Confidential Match source code and images are built with the following
dependencies:

* Debian 11 (Bullseye) on x86_64
* Docker Engine 20.10.21
    * https://docs.docker.com/engine/install/debian/#install-using-the-repository
    * https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user
* Bazel 6.4.0
    * https://bazel.build/versions/6.4.0/install/ubuntu
    * If installing Bazelisk, it must be callable with the command `bazel`
* OpenJDK JRE 11
* Python 3.8.13
    * Installed using https://github.com/pyenv/pyenv
* Go (golang) 1.18.2
* Google Cloud SDK (gcloud) 426.0.0
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

### How to Build the Match Request Processor (MRP)

Run `build_mrp.sh` to build the image and load it into Docker
as `bazel/java/com/google/cm/mrp:mrp_app_gcp_image`.

## How to Verify Image Signatures

Images are signed with [cosign](https://github.com/sigstore/cosign), which
stores signatures in a
[public repository](https://us-docker.pkg.dev/admcloud-cfm-public/docker-repo-signatures).
Verify a signature by following these steps:

1. Install
   [cosign v1.11.1](https://github.com/sigstore/cosign/releases/tag/v1.11.1)
2. Build the image to be verified using steps from the sections above,
   either [MRP](#how-to-build-the-match-request-processor--mrp-)
   or [Lookup Service](#how-to-build-the-lookup-service)
3. Push the image to a repository using Docker, generating a digest that should
   match the released image digest
    * Note that pushing with a different tool can generate an unexpected digest
    * Choose a repository that you own that is not public
    * Here are example commands to push an MRP image built with the steps
      mentioned above, using Docker to push the image to the fake repository
      `us-docker.pkg.dev/cfm-verification-examples/mrp-example:v1.2.3`:

    ```bash
    $ docker tag bazel/java/com/google/cm/mrp:mrp_app_gcp_image us-docker.pkg.dev/cfm-verification-examples/mrp-example:v1.2.3
    $ docker push us-docker.pkg.dev/cfm-verification-examples/mrp-example:v1.2.3
    ```

4. Then run one of the following commands from this Git repository root after
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

    ```
    Verification for us-docker.pkg.dev/cfm-verification-examples/mrp-example:v1.2.3 --
    The following checks were performed on each of these signatures:
    - The specified annotations were verified.
    - The cosign claims were validated
    - The signatures were verified against the specified public key

    [{"critical":{"identity":{"docker-reference":"us-docker.pkg.dev/admcloud-cfm-services/docker-repo/mrp_app_gcp"},"image":{"docker-manifest-digest":"sha256:d719135212b741e3a1ccea71d6bd22351d0730c3b9a84967caf1e9a2f7f083b7"},"type":"cosign container image signature"},"optional":{"dev.cosignproject.cosign/pub":"LS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS0KTUZrd0V3WUhLb1pJemowQ0FRWUlLb1pJemowREFRY0RRZ0FFUm93NllsOVpyOFJyNWNvd1MvTDVTOHE4d1ROZQpYOUxaTUIxaXBhelFmN0pQNDFsYWthUHlCdDFKK3hyZUpKYW5RLy9wZExwczh6SUg1ZFBNTkFEdnN3PT0KLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tCg","dev.cosignproject.cosign/sigalg":"ECDSA_P256_SHA256"}}]
    ```
