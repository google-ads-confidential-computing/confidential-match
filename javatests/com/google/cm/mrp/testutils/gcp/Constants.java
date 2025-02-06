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

package com.google.cm.mrp.testutils.gcp;

/** Constants used across classes in this package. */
public final class Constants {

  private Constants() {}

  /** Test KEK in JSON cleartext to use for integration tests. (Previously generated) */
  public static final String TEST_KEK_JSON =
      "{\"primaryKeyId\":219896464,\"key\":[{\"keyData\":{\"typeUrl\":\"type.googleapis.com/google.crypto.tink.XChaCha20Poly1305Key\",\"value\":\"GiAtgk5fBFHv5sTm+p6TFsAT7+DwIiGwFEv+CVFEpz9vUQ==\",\"keyMaterialType\":\"SYMMETRIC\"},\"status\":\"ENABLED\",\"keyId\":219896464,\"outputPrefixType\":\"TINK\"}]}\n";

  /**
   * Test encrypted Hybrid private keyset in JSON cleartext to use for integration tests.(Previously
   * generated) Note: The keyset JSON cleartext is encrypted with above AEAD.
   */
  public static final String TEST_HYBRID_PRIVATE_KEYSET =
      "{\"encryptedKeyset\":\"AQ0bWpDaxQ4RSyEjXGazp+LtrKwSgBcHq7LXUUIRdrVKxijGxIMOMnfwZsJ3mWVVRNFdth2Fgxaa98Gq8+hJHeRsVgQbeaoWZEUxyED7ougoiovrhEy1cADRiKpKF8NQbHRWoeJgdnRyqK0uqosfSAxpsamX2jzVXvVmN0uZRppywH08jcYF1IBc6qQyG6J7MngSfH5bMs7ApmmqZmU3ikw2WxxnFvYGGwpgQvrXD5laMafuzN/A5nKssyUvJvenL6Gu8VQWkDGZoQl8\",\"keysetInfo\":{\"primaryKeyId\":1126295029,\"keyInfo\":[{\"typeUrl\":\"type.googleapis.com/google.crypto.tink.HpkePrivateKey\",\"status\":\"ENABLED\",\"keyId\":1126295029,\"outputPrefixType\":\"RAW\"}]}}";

  /** Fake project ID used by emulated resources. */
  static final String PROJECT_ID = "test-services-project";

  /** Frontend job version. */
  static final int JOB_VERSION = 1;

  /** Topic created for the PubSub emulator. */
  static final String PUBSUB_TOPIC_ID = "test-job-topic";

  /** Subscription created for the PubSub emulator. */
  static final String PUBSUB_SUBSCRIPTION_ID = "test-job-subscription";

  /**
   * Maximum size of PubSub messages in bytes. This is ignored by the worker client (it always uses
   * 1000), but a value must still be provided.
   */
  static final int PUBSUB_MAX_MESSAGE_SIZE_BYTES = 1000;

  /** Number of seconds a PubSub message lease lasts. */
  static final int PUBSUB_MESSAGE_LEASE_SECONDS = 30;

  /** Name of the emulated Spanner instance. */
  static final String SPANNER_INSTANCE_NAME = "test-job-instance";

  /** Name of the emulated Spanner database. */
  static final String SPANNER_DATABASE_NAME = "test-job-database";

  /** Maximum attempts to process a job from the Queue. */
  static final int JOB_QUEUE_MAX_ATTEMPTS = 1;

  /** Job Queue retry delay seconds. */
  static final int JOB_QUEUE_RETRY_DELAY_SEC = 10;

  /** Threads available for data processing. */
  static final int MRP_THREADS = 1;

  /** Number of times to retry job-level errors. */
  static final int MRP_JOB_PROCESSOR_RETRIES = 2;

  /** Maximum records for each data chunk. */
  static final int DATA_CHUNK_SIZE = 1000;

  /** Maximum records for each output file. */
  static final int MAX_RECORDS_PER_OUTPUT_FILE = 1000000;

  /** Name of the cluster group used for testing. */
  static final String CLUSTER_GROUP_ID = "local";

  /** Lookup service audience used for testing. */
  static final String LOOKUP_SERVICE_AUDIENCE = "lookup-server-local";

  /** Maximum records for each lookup request. */
  static final int LOOKUP_CLIENT_MAX_RECORDS_PER_REQUEST = 1000;

  /** Maximum retries for each lookup request. */
  static final int LOOKUP_SERVICE_MAX_RETRIES = 1; // Retry count *after* first attempt

  /** Threads available for executing lookup requests. */
  static final int LOOKUP_CLIENT_THREADS = 1;

  /** Flag to enable MIC feature */
  static final boolean LOCAL_MIC_FEATURE_ENABLED = true;
}
