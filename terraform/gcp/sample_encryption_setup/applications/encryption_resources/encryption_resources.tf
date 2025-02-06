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

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4.84"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
  zone    = var.zone
}

################################################################################
# Workload Identity Pool
################################################################################

resource "google_iam_workload_identity_pool" "workload_identity_pool" {
  # ID and display name are limited to a maximum length of 32
  workload_identity_pool_id = "${var.environment}-wip"
  display_name              = "${var.environment}-wip"
  description               = "Workload Identity Pool to manage key access using attestation."
}

locals {
  # For use in provider. Other resources should use provider attributes.
  wip_provider_id = "${var.environment}-wip-pvdr"
}

resource "google_iam_workload_identity_pool_provider" "wip_provider" {
  # ID and display name are limited to a maximum length of 32
  workload_identity_pool_provider_id = local.wip_provider_id
  display_name                       = local.wip_provider_id
  description                        = "WIP Provider to manage OIDC tokens and attestation."

  workload_identity_pool_id = google_iam_workload_identity_pool.workload_identity_pool.workload_identity_pool_id

  # Mapping grants access to subset of identities
  # https://cloud.google.com/iam/docs/workload-identity-federation?_ga=2.215242071.-153658173.1651444935#mapping
  attribute_mapping = {
    "google.subject" : "assertion.sub"
    # Set group to the provider ID so the provider can be referenced in the IAM principalSet for KMS decryption permission
    "google.groups" : "[\"${local.wip_provider_id}\"]"
  }

  # Access conditions
  # https://cloud.google.com/iam/docs/workload-identity-federation?_ga=2.39606883.-153658173.1651444935#conditions
  # https://cloud.google.com/confidential-computing/confidential-space/docs/create-grant-access-confidential-resources#container-assertions
  attribute_condition = <<-EOT
  assertion.swname == 'CONFIDENTIAL_SPACE'
   && 'STABLE' in assertion.submods.confidential_space.support_attributes
   && ['${join("','", var.allowed_service_account_emails)}'].exists(a, a in assertion.google_service_accounts)
   && '${var.image_signature_algorithm}:${var.image_signature_public_key_fingerprint}' in assertion.submods.container.image_signatures.map(sig, sig.signature_algorithm+':'+sig.key_id)
  EOT

  oidc {
    # Only allow STS requests from confidential computing
    allowed_audiences = ["https://sts.googleapis.com"]
    issuer_uri        = "https://confidentialcomputing.googleapis.com"
  }
}

################################################################################
# KMS Key
################################################################################

resource "google_kms_key_ring" "key_ring" {
  name     = "${var.environment}-keyring"
  location = var.region
}

resource "google_kms_crypto_key" "crypto_key" {
  name     = "${var.environment}-encryption-key"
  key_ring = google_kms_key_ring.key_ring.id

  purpose         = "ENCRYPT_DECRYPT"
  rotation_period = "7776000s" # 90 days

  version_template {
    algorithm        = "GOOGLE_SYMMETRIC_ENCRYPTION"
    protection_level = "SOFTWARE"
  }
}

resource "google_kms_crypto_key_iam_member" "verified_key_decrypter" {
  crypto_key_id = google_kms_crypto_key.crypto_key.id
  member        = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.workload_identity_pool.name}/group/${google_iam_workload_identity_pool_provider.wip_provider.workload_identity_pool_provider_id}"
  role          = "roles/cloudkms.cryptoKeyDecrypter"
}
