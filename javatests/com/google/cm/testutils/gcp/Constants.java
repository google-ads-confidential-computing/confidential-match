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

package com.google.cm.testutils.gcp;

import com.google.common.collect.ImmutableList;

/** Constants for use with test utilities. */
public final class Constants {

  private Constants() {}

  /** Spanner DDL that can be used to start a job database emulator. */
  public static final ImmutableList<String> JOB_DB_DDL =
      ImmutableList.of(
          "CREATE TABLE JobMetadata ("
              + " JobKey STRING(256) NOT NULL,"
              + " RequestInfo JSON NOT NULL,"
              + " JobStatus STRING(64) NOT NULL,"
              + " ServerJobId STRING(50) NOT NULL,"
              + " NumAttempts INT64 NOT NULL,"
              + " RequestReceivedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),"
              + " RequestUpdatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),"
              + " RequestProcessingStartedAt TIMESTAMP OPTIONS (allow_commit_timestamp=true),"
              + " TargetWorkgroup STRING(256),"
              + " ResultInfo JSON,"
              + " Ttl TIMESTAMP NOT NULL,"
              + ") PRIMARY KEY (JobKey),"
              + " ROW DELETION POLICY (OLDER_THAN(Ttl, INTERVAL 0 DAY))");
}
