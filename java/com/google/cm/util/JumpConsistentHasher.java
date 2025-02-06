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

package com.google.cm.util;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;

/**
 * Defines a wrapper class for Guava's implementation of Jump Consistent Hashing based on the
 * algorithm by Lamping, Veach <a href="https://arxiv.org/pdf/1406.2294.pdf">hyperlinks</a>
 */
public class JumpConsistentHasher {

  private JumpConsistentHasher() {}

  /**
   * Performs jump consistent hash for a given key and a set number of buckets. Returns the bucket
   * assigned to this key.
   */
  public static int hash(String input, int numBuckets) {
    // Get HashCode from string bytes to allow wider kinds of inputs
    HashCode hashCode = HashCode.fromBytes(input.getBytes(StandardCharsets.UTF_8));
    return Hashing.consistentHash(hashCode, numBuckets);
  }
}
