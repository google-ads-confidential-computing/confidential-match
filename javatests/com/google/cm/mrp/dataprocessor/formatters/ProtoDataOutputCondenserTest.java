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

package com.google.cm.mrp.dataprocessor.formatters;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cm.mrp.backend.CondensedResponseColumnProto.CondensedResponseColumn;
import com.google.cm.mrp.backend.CondensedResponseColumnProto.CondensedResponseColumn.Column;
import com.google.cm.mrp.backend.CondensedResponseColumnProto.CondensedResponseColumn.ColumnGroup;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord.KeyValue;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.util.ProtoUtils;
import com.google.common.io.Resources;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ProtoDataOutputCondenserTest {
  private ProtoDataOutputCondenser protoDataOutputCondenser;
  Map<String, String> compositeColumnMap; // Condenser input
  private Map<String, String> ungroupedPiiMap; // Result validation
  private List<Map<String, String>> addressGroupMaps; // Result validation
  private List<Map<String, String>> socialGroupMaps; // Result validation
  private List<Integer> indices;
  private List<DataRecord> dataRecords;
  private Schema flatSchema;

  @Before
  public void setup() throws Exception {
    Schema jobSchema = getSchema("testdata/nested_data_output_condenser_schema.json");
    flatSchema = getSchema("testdata/flat_schema.json");
    protoDataOutputCondenser = new ProtoDataOutputCondenser(jobSchema);

    dataRecords = new ArrayList<>();
    List<KeyValue> record1 = new ArrayList<>();
    record1.add(KeyValue.newBuilder().setKey("em").setStringValue("FAKE.1@google.com").build());
    record1.add(KeyValue.newBuilder().setKey("pn").setStringValue("555-555-5551").build());
    record1.add(KeyValue.newBuilder().setKey("fn").setStringValue("John").build());
    record1.add(KeyValue.newBuilder().setKey("ln").setStringValue("Doe").build());
    record1.add(KeyValue.newBuilder().setKey("pc").setStringValue("99999").build());
    record1.add(KeyValue.newBuilder().setKey("co").setStringValue("US").build());
    record1.add(KeyValue.newBuilder().setKey("ig").setStringValue("@fake-insta").build());
    record1.add(KeyValue.newBuilder().setKey("tt").setStringValue("@fake-tiktok").build());
    record1.add(KeyValue.newBuilder().setKey("coordinator_key").setStringValue("123").build());
    record1.add(KeyValue.newBuilder().setKey("metadata").setStringValue("metadata").build());
    record1.add(KeyValue.newBuilder().setKey("error_codes").setStringValue("123").build());
    dataRecords.add(DataRecord.newBuilder().addAllKeyValues(record1).build());
    List<KeyValue> record2 = new ArrayList<>();
    record2.add(KeyValue.newBuilder().setKey("em").setStringValue("FAKE.2@google.com").build());
    record2.add(KeyValue.newBuilder().setKey("pn").setStringValue("555-555-5552").build());
    record2.add(KeyValue.newBuilder().setKey("fn").setStringValue("Jane").build());
    record2.add(KeyValue.newBuilder().setKey("ln").setStringValue("Deer").build());
    record2.add(KeyValue.newBuilder().setKey("pc").setStringValue("V6Z 2H7").build());
    record2.add(KeyValue.newBuilder().setKey("co").setStringValue("CA").build());
    record2.add(KeyValue.newBuilder().setKey("ig").setStringValue("").build());
    record2.add(KeyValue.newBuilder().setKey("tt").setStringValue("").build());
    record2.add(KeyValue.newBuilder().setKey("coordinator_key").setStringValue("123").build());
    record2.add(KeyValue.newBuilder().setKey("metadata").setStringValue("").build());
    record2.add(KeyValue.newBuilder().setKey("error_codes").setStringValue("123").build());
    dataRecords.add(DataRecord.newBuilder().addAllKeyValues(record2).build());
    List<KeyValue> record3 = new ArrayList<>();
    record3.add(KeyValue.newBuilder().setKey("em").setStringValue("FAKE.3@google.com").build());
    record3.add(KeyValue.newBuilder().setKey("pn").setStringValue("").build());
    record3.add(KeyValue.newBuilder().setKey("fn").setStringValue("UNMATCHED_1").build());
    record3.add(KeyValue.newBuilder().setKey("ln").setStringValue("UNMATCHED_2").build());
    record3.add(KeyValue.newBuilder().setKey("pc").setStringValue("UNMATCHED_3").build());
    record3.add(KeyValue.newBuilder().setKey("co").setStringValue("").build());
    record3.add(KeyValue.newBuilder().setKey("ig").setStringValue("").build());
    record3.add(KeyValue.newBuilder().setKey("tt").setStringValue("").build());
    record3.add(KeyValue.newBuilder().setKey("coordinator_key").setStringValue("123").build());
    record3.add(KeyValue.newBuilder().setKey("metadata").setStringValue("").build());
    record3.add(KeyValue.newBuilder().setKey("error_codes").setStringValue("123").build());
    dataRecords.add(DataRecord.newBuilder().addAllKeyValues(record3).build());

    compositeColumnMap = new HashMap<>();
    compositeColumnMap.put("fn", "address");
    compositeColumnMap.put("ln", "address");
    compositeColumnMap.put("pc", "address");
    compositeColumnMap.put("co", "address");
    compositeColumnMap.put("ig", "socials");
    compositeColumnMap.put("tt", "socials");

    ungroupedPiiMap = new HashMap<>();
    ungroupedPiiMap.put("FAKE.1@google.com", "em");
    ungroupedPiiMap.put("FAKE.2@google.com", "em");
    ungroupedPiiMap.put("FAKE.3@google.com", "em");
    ungroupedPiiMap.put("555-555-5551", "pn");
    ungroupedPiiMap.put("555-555-5552", "pn");

    addressGroupMaps = new ArrayList<>();
    Map<String, String> firstAddressGroupMap = new HashMap<>();
    firstAddressGroupMap.put("John", "fn");
    firstAddressGroupMap.put("Doe", "ln");
    firstAddressGroupMap.put("99999", "pc");
    firstAddressGroupMap.put("US", "co");
    addressGroupMaps.add(firstAddressGroupMap);
    Map<String, String> secondAddressGroupMap = new HashMap<>();
    secondAddressGroupMap.put("Jane", "fn");
    secondAddressGroupMap.put("Deer", "ln");
    secondAddressGroupMap.put("V6Z 2H7", "pc");
    secondAddressGroupMap.put("CA", "co");
    addressGroupMaps.add(secondAddressGroupMap);
    Map<String, String> thirdAddressGroupMap = new HashMap<>();
    thirdAddressGroupMap.put("UNMATCHED_1", "fn");
    thirdAddressGroupMap.put("UNMATCHED_2", "ln");
    thirdAddressGroupMap.put("UNMATCHED_3", "pc");
    addressGroupMaps.add(thirdAddressGroupMap);

    socialGroupMaps = new ArrayList<>();
    Map<String, String> socialsGroupMap = new HashMap<>();
    socialsGroupMap.put("@fake-insta", "ig");
    socialsGroupMap.put("@fake-tiktok", "tt");
    socialGroupMaps.add(socialsGroupMap);
  }

  @Test
  public void dataOutputCondenser_correctCondensing() {
    KeyValue condensedKeyValue = protoDataOutputCondenser.condense(dataRecords, flatSchema);

    Optional<CondensedResponseColumn> condensedResponseColumnProto =
        base64Decode(condensedKeyValue.getStringValue());
    List<Column> columns =
        condensedResponseColumnProto
            .map(CondensedResponseColumn::getColumnsList)
            .orElseGet(List::of);
    assertEquals(9, columns.size());
    columns.forEach(
        column -> {
          if (column.hasColumn()) {
            CondensedResponseColumn.KeyValue keyValue = column.getColumn();
            assertEquals(keyValue.getKey(), ungroupedPiiMap.get(keyValue.getValue()));
            ungroupedPiiMap.remove(keyValue.getValue());
          } else {
            ColumnGroup columnGroup = column.getColumnGroup();
            switch (columnGroup.getName()) {
              case "address":
                assertTrue(findGroups(columnGroup.getSubcolumnsList(), addressGroupMaps));
                break;
              case "socials":
                assertTrue(findGroups(columnGroup.getSubcolumnsList(), socialGroupMaps));
                break;
              default:
                fail("Unknown ColumnGroup name found: " + columnGroup.getName());
            }
          }
        });
    assertEquals(0, ungroupedPiiMap.size());
    addressGroupMaps.forEach(addressGroup -> assertEquals(0, addressGroup.size()));
    socialGroupMaps.forEach(socialGroup -> assertEquals(0, socialGroup.size()));
  }

  private boolean findGroups(
      List<CondensedResponseColumn.KeyValue> subcolumns, List<Map<String, String>> groupMaps) {
    for (var groupMap : groupMaps) {
      if (groupMap.containsKey(subcolumns.get(0).getValue())) {
        for (var keyValue : subcolumns) {
          assertEquals(keyValue.getKey(), groupMap.get(keyValue.getValue()));
          groupMap.remove(keyValue.getValue());
        }
        return 0 == groupMap.size();
      }
    }
    return false;
  }

  private Optional<CondensedResponseColumn> base64Decode(String base64EncodedString) {
    try {
      byte[] decodedBytes = Base64.getDecoder().decode(base64EncodedString);
      return Optional.of(CondensedResponseColumn.parseFrom(decodedBytes));
    } catch (Exception e) {
      fail("Failed to decode CondensedResponseColumn: " + e.getMessage());
      return Optional.empty();
    }
  }

  private Schema getSchema(String path) throws Exception {
    return ProtoUtils.getProtoFromJson(
        Resources.toString(Objects.requireNonNull(getClass().getResource(path)), UTF_8),
        Schema.class);
  }
}
