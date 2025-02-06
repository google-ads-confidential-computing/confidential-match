# CFM Encryption Resources Terraform Script

## Prerequisites

* Terraform version 4.84 or higher is required.
* A GCS bucket must be manually created for the Terraform backend.
* GCP's `Cloud Key Management Service (KMS) API` must be manually enabled.
* Terraform will use GCP application default credentials, unless a path to
  alternate credentials is provided with the `GOOGLE_APPLICATION_CREDENTIALS`
  environment variable. See
  https://registry.terraform.io/providers/hashicorp/google/latest/docs/guides/getting_started#configuring-the-provider
  and
  https://registry.terraform.io/providers/hashicorp/google/latest/docs/guides/getting_started#adding-credentials
  for more information.
* The credentials used must have the following GCP IAM roles:
  * `Storage Admin` for the chosen GCS backend location
  * `IAM Workload Identity Pool Admin`
  * `Cloud KMS Admin`

## Configuring Environments

Folders in `environments/` represent environments, each one with a distinct
deployment configuration. Deploying with Terraform requires configuring a new
environment folder. An example environment has been provided to demonstrate how
to set it up. An environment folder must contain the following files:

* `main.tf` with a Terraform backend configured for an existing GCS bucket and
  prefix, unique for every environment. This file should also contain an exact
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
within the key. Workload identity pools and their providers aren't immediately
deleted, but instead enter a `DELETED` state for 30 days before being
permanently deleted.

Terraform will fail if it tries to create a resource that already exists. Keys
and key rings can be restored with `terraform import` commands. WIP resources
will need to be undeleted before they can be imported.
