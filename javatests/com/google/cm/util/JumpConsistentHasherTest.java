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

import static com.google.common.truth.Truth.assertWithMessage;

import com.google.common.collect.Range;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class JumpConsistentHasherTest {

  private final long randomSeed = new Random().nextInt();

  @Test
  public void addingBucketDoesNotReshuffleToOldBuckets() {
    List<String> items = getListWithRandomValues(10000);
    Map<String, Integer> oldBuckets = createBucketMap(10, items);

    // Add a bucket
    Map<String, Integer> newBuckets = createBucketMap(11, items);

    oldBuckets.forEach(
        (key, oldBucket) -> {
          int newBucket = newBuckets.get(key);
          // Key should either be in its old bucket or in the newly added bucket (#10).
          assertWithMessage("randomSeed: %s", randomSeed).that(newBucket).isAnyOf(oldBucket, 10);
        });
  }

  @Test
  public void removingBucketOnlyMovesItemsInRemovedBucket() {
    List<String> items = getListWithRandomValues(10000);
    Map<String, Integer> oldBuckets = createBucketMap(10, items);

    // Remove a bucket
    Map<String, Integer> newBuckets = createBucketMap(9, items);

    oldBuckets.forEach(
        (key, oldBucket) -> {
          int newBucket = newBuckets.get(key);
          // Keys that used to be in the removed bucket can be in any bucket 0-8.
          // Every other key should have stayed in the same bucket.
          if (oldBucket == 9) {
            assertWithMessage("randomSeed: %s", randomSeed)
                .that(newBucket)
                .isIn(Range.closed(0, 8));
          } else {
            assertWithMessage("randomSeed: %s", randomSeed).that(newBucket).isEqualTo(oldBucket);
          }
        });
  }

  @Test
  public void bucketSizesAreSimilar() {
    int numBuckets = 20;
    Map<String, Integer> buckets = createBucketMap(numBuckets, getListWithRandomValues(100000));

    // Each bucket should have around 5000 items (100,000/20)
    IntStream.range(0, numBuckets)
        .forEach(
            bucket -> {
              int bucketSize = Collections.frequency(buckets.values(), bucket);
              assertWithMessage("randomSeed: %s", randomSeed)
                  .that(bucketSize)
                  .isIn(Range.closed(4500, 5500));
            });
    assertWithMessage("randomSeed: %s", randomSeed)
        .that(Collections.max(buckets.values()))
        .isEqualTo(numBuckets - 1);
  }

  private Map<String, Integer> createBucketMap(int numBuckets, List<String> items) {
    Map<String, Integer> buckets = new HashMap<>();
    items.forEach(item -> buckets.put(item, JumpConsistentHasher.hash(item, numBuckets)));
    return buckets;
  }

  private List<String> getListWithRandomValues(int size) {
    Random random = new Random(randomSeed);
    List<String> items = new ArrayList<>();
    IntStream.range(0, size)
        .forEach(
            unused -> {
              String newItem = random.nextLong() + "@email.com";
              items.add(
                  Hashing.sha256().hashBytes(newItem.getBytes(StandardCharsets.UTF_8)).toString());
            });
    return items;
  }
}
