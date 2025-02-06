/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cm.mrp;

/** MRP parameters that can be read by the parameter client. */
public enum Parameter {
  MRP_THREAD_POOL_SIZE,
  LOOKUP_CLIENT_THREAD_POOL_SIZE,
  INPUT_DATA_CHUNK_SIZE,
  LOOKUP_CLIENT_CLUSTER_GROUP_ID,
  LOOKUP_CLIENT_MAX_RECORDS_PER_REQUEST,
  LOOKUP_CLIENT_MAX_REQUEST_RETRIES,
  ORCHESTRATOR_ENDPOINT,
  LOOKUP_SERVICE_AUDIENCE,
  MAX_RECORDS_PER_OUTPUT_FILE,
  LOOKUP_PROTO_FORMAT,
  JOB_PROCESSOR_MAX_RETRIES,
  JOB_QUEUE_RETRY_DELAY_SEC,
  CONSCRYPT_ENABLED,

  // MRP add-on feature flags
  MIC_FEATURE_ENABLED,
  // Serialized proto feature flag
  SERIALIZED_PROTO_FEATURE_ENABLED,
  // Coordinator batch encryption feature flag
  COORDINATOR_BATCH_ENCRYPTION_ENABLED;

  /** String to prefix the parameters. */
  public static final String CFM_PREFIX = "CFM";

  /**
   * Application specific flag for job notification topic, stored in the parameter store in this
   * format: CFM-{environment}-NOTIFICATION_TOPIC_{APPLICATION_ID}
   */
  public static final String NOTIFICATION_TOPIC_PREFIX = "NOTIFICATION_TOPIC_";
}
