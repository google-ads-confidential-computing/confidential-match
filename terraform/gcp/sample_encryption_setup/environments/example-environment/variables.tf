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
# Global Variables
################################################################################

variable "environment" {
  description = "Name of the environment containing this resource (e.g. dev, prod)."
  type        = string
  validation {
    condition     = length(var.environment) <= 23 && length(var.environment) > 0
    error_message = "Environment must be 1 to 23 characters long due to resource name limitations."
  }
}

variable "project" {
  description = "Name of the GCP project containing this resource."
  type        = string
}

variable "region" {
  description = "Name of the region containing this resource."
  type        = string
}

variable "zone" {
  description = "Name of the region zone containing this resource."
  type        = string
}

################################################################################
# Workload Identity Pool Variables
################################################################################

variable "allowed_service_account_emails" {
  description = "Emails of existing service accounts that will be allowed to access the workload identity pool."
  type        = list(string)
}

variable "image_signature_algorithm" {
  description = "The algorithm used to sign the image, e.g. ECDSA_P256_SHA256."
  type        = string
}

variable "image_signature_public_key_fingerprint" {
  description = "Hexadecimal fingerprint of the public key used to sign the image."
  type        = string
}
