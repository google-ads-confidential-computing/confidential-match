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

package com.google.cm.mrp.dataprocessor;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cm.mrp.MatchConfigProvider;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.backend.SchemaProto.Schema.Column;
import com.google.cm.mrp.dataprocessor.models.ColumnGroupIndices;
import com.google.cm.mrp.dataprocessor.models.MatchColumnIndices;
import com.google.cm.mrp.dataprocessor.models.SingleColumnIndices;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MatchColumnsListTest {
  @Test
  public void generateMatchColumnsListForDataSource1_success() {
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    Schema schema = getSchema(testData);
    MatchConfig matchConfig = MatchConfigProvider.getMatchConfig("customer_match");

    MatchColumnsList matchColumnsList =
        MatchColumnsList.generateMatchColumnsListForDataSource1(schema, matchConfig);

    assertEquals(3, matchColumnsList.getList().size());
    List<MatchColumnIndices> expectedMatchColumnsList = new ArrayList<>();
    expectedMatchColumnsList.add(
        MatchColumnIndices.ofSingleColumnIndices(SingleColumnIndices.create(ImmutableList.of(0))));
    expectedMatchColumnsList.add(
        MatchColumnIndices.ofSingleColumnIndices(SingleColumnIndices.create(ImmutableList.of(1))));
    ListMultimap<Integer, Integer> expectedColumnGroupIndicesMap = ArrayListMultimap.create();
    expectedColumnGroupIndicesMap.put(0, 2);
    expectedColumnGroupIndicesMap.put(0, 3);
    expectedColumnGroupIndicesMap.put(0, 5);
    expectedColumnGroupIndicesMap.put(0, 4);
    expectedMatchColumnsList.add(
        MatchColumnIndices.ofColumnGroupIndices(
            ColumnGroupIndices.create(expectedColumnGroupIndicesMap)));
    assertThat(matchColumnsList.getList()).containsExactlyElementsIn(expectedMatchColumnsList);
  }

  @Test
  public void generateMatchColumnsListForDataSource1_duplicatePiiTypes_success() {
    String[][] testData = {
      {"email1", "email", "fake.email@google.com"},
      {"email2", "email", "fake.email@google.com"},
      {"phone1", "phone", "999-999-9999"},
      {"phone2", "phone", "999-999-9999"},
      {"first_name1", "first_name", "fake_first_name", "0"},
      {"first_name2", "first_name", "fake_first_name", "1"},
      {"last_name1", "last_name", "fake_last_name", "0"},
      {"last_name2", "last_name", "fake_last_name", "1"},
      {"zip_code1", "zip_code", "99999", "0"},
      {"zip_code2", "zip_code", "99999", "1"},
      {"country_code1", "country_code", "US", "0"},
      {"country_code2", "country_code", "US", "1"},
    };
    Schema schema = getSchema(testData);
    MatchConfig matchConfig = MatchConfigProvider.getMatchConfig("customer_match");

    MatchColumnsList matchColumnsList =
        MatchColumnsList.generateMatchColumnsListForDataSource1(schema, matchConfig);
    assertEquals(3, matchColumnsList.getList().size());
    List<MatchColumnIndices> expectedMatchColumnsList = new ArrayList<>();
    expectedMatchColumnsList.add(
        MatchColumnIndices.ofSingleColumnIndices(
            SingleColumnIndices.create(ImmutableList.of(0, 1))));
    expectedMatchColumnsList.add(
        MatchColumnIndices.ofSingleColumnIndices(
            SingleColumnIndices.create(ImmutableList.of(2, 3))));
    ListMultimap<Integer, Integer> expectedColumnGroupIndicesMap = ArrayListMultimap.create();
    expectedColumnGroupIndicesMap.put(0, 4);
    expectedColumnGroupIndicesMap.put(0, 6);
    expectedColumnGroupIndicesMap.put(0, 10);
    expectedColumnGroupIndicesMap.put(0, 8);
    expectedColumnGroupIndicesMap.put(1, 5);
    expectedColumnGroupIndicesMap.put(1, 7);
    expectedColumnGroupIndicesMap.put(1, 11);
    expectedColumnGroupIndicesMap.put(1, 9);
    expectedMatchColumnsList.add(
        MatchColumnIndices.ofColumnGroupIndices(
            ColumnGroupIndices.create(expectedColumnGroupIndicesMap)));
    assertThat(matchColumnsList.getList()).containsExactlyElementsIn(expectedMatchColumnsList);
  }

  @Test
  public void isMatchColumn_true() {
    String[][] testData = {
      {"userId", "userId", "user1"},
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    Schema schema = getSchema(testData);
    MatchConfig matchConfig = MatchConfigProvider.getMatchConfig("customer_match");

    MatchColumnsList matchColumnsList =
        MatchColumnsList.generateMatchColumnsListForDataSource1(schema, matchConfig);

    for (int i = 1; i < 7; i++) {
      assertTrue(matchColumnsList.isMatchColumn(i));
    }
  }

  @Test
  public void isMatchColumn_false() {
    String[][] testData = {
      {"userId", "userId", "user1"},
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    Schema schema = getSchema(testData);
    MatchConfig matchConfig = MatchConfigProvider.getMatchConfig("customer_match");

    MatchColumnsList matchColumnsList =
        MatchColumnsList.generateMatchColumnsListForDataSource1(schema, matchConfig);

    assertFalse(matchColumnsList.isMatchColumn(0));
    assertFalse(matchColumnsList.isMatchColumn(7));
  }

  @Test
  public void countPiis_success() {
    String[][] testData = {
      {"userId", "userId", "user1"},
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    Schema schema = getSchema(testData);
    MatchConfig matchConfig = MatchConfigProvider.getMatchConfig("customer_match");

    MatchColumnsList matchColumnsList =
        MatchColumnsList.generateMatchColumnsListForDataSource1(schema, matchConfig);

    assertThat(matchColumnsList.countPiis()).isEqualTo(6);
  }

  private Schema getSchema(String[][] keyValueQuads) {
    Schema.Builder builder = Schema.newBuilder();
    for (String[] keyValue : keyValueQuads) {
      builder.addColumns(
          Column.newBuilder()
              .setColumnName(keyValue[0])
              .setColumnAlias(keyValue[1])
              .setColumnGroup(keyValue.length > 3 ? Integer.parseInt(keyValue[3]) : 0)
              .build());
    }
    return builder.build();
  }
}
