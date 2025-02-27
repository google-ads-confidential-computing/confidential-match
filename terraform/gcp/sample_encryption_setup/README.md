# Steps to Set Up GCP Environment to Encrypt Data

This directory contains an example Terraform configuration that can be deployed
to encrypt data. It configures a Google Cloud KMS key and a Workload Identity
Pool that controls decryption access to the key.

## Prerequisites

* Terraform version 4.84 or higher is required.
* A GCS bucket must be manually created for the Terraform backend.
* GCP's `Cloud Key Management Service (KMS) API` must be manually enabled.
* The credentials used must have the following GCP IAM roles:
  * `Storage Admin` for the chosen GCS backend location
  * `IAM Workload Identity Pool Admin`
  * `Cloud KMS Admin`
* Terraform will use GCP application default credentials, unless a path to
  alternate credentials is provided with the `GOOGLE_APPLICATION_CREDENTIALS`
  environment variable. See
  https://registry.terraform.io/providers/hashicorp/google/latest/docs/guides/getting_started#configuring-the-provider
  and
  https://registry.terraform.io/providers/hashicorp/google/latest/docs/guides/getting_started#adding-credentials
  for more information.

## Configuring Environments

Folders in `environments/` represent environments, each one with a distinct
deployment configuration. Deploying with Terraform requires configuring a new
environment folder. An example environment has been provided to demonstrate how
to set it up. An environment folder must contain the following files:

* `main.tf` with a Terraform backend configured for an existing GCS bucket and
  prefix. The prefix should be unique for every environment.
  * This file should also contain an exact
    copy of the example's `module "encryption_resources"` block.
* `outputs.tf` and `variables.tf` exactly like the example.
* `{{environment}}.auto.tfvars` with values for all variables in `variables.tf`.

## Deploying resources

From an environment folder, run `terraform init` followed by `terraform apply`.

## Destroying resources

From an environment folder, run `terraform destroy`.

Be aware that some resources cannot be destroyed or recreated easily. Key rings
cannot be deleted, so the destroy command will only remove it from Terraform's
state. Keys work the same way, but the command will also delete all key versions
within the key. Workload identity pools and their providers are not immediately
deleted but are instead set to a `DELETED` state for 30 days before being
permanently deleted.

Terraform will fail if it tries to create a resource that already exists. Keys
and key rings can be restored with `terraform import` commands. Workload
identity pool resources will need to be restored before they can be imported.
