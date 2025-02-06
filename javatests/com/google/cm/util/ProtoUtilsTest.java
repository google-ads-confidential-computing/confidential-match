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

import static com.google.cm.util.ProtoUtils.getJsonFromProto;
import static com.google.cm.util.ProtoUtils.getProtoFromJson;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class ProtoUtilsTest {

  @Test
  public void getJsonFromProto_returnsJson() throws Exception {
    String expected = "someKindOfString"; // this counts as a JSON version of StringValue
    StringValue proto = StringValue.newBuilder().setValue(expected).build();

    String json = getJsonFromProto(proto);

    assertThat(json).contains(expected);
  }

  @Test
  public void getJsonFromProto_invalidProto_throwsException() {
    String expected = "someKindOfTypeUrl";
    Any proto = Any.newBuilder().setTypeUrl(expected).build();

    var ex = assertThrows(InvalidProtocolBufferException.class, () -> getJsonFromProto(proto));

    assertThat(ex).hasMessageThat().isEqualTo("Invalid type url found: " + expected);
  }

  @Test
  public void getProtoFromJson_returnsProto() throws Exception {
    String expected = "someKindOfString"; // this counts as a JSON version of StringValue

    var proto = getProtoFromJson(expected, StringValue.class);

    assertThat(proto.getValue()).isEqualTo(expected);
  }

  @Test
  public void getProtoFromJson_invalidJson_throwsException() {
    String json = "not-json";

    var ex =
        assertThrows(InvalidProtocolBufferException.class, () -> getProtoFromJson(json, Any.class));

    assertThat(ex).hasMessageThat().isEqualTo("Expect message object but got: \"" + json + "\"");
  }

  @Test
  public void getProtoFromJson_newBuilderNotImplemented_throwsException() {
    String json = "ignored";

    var ex =
        assertThrows(
            InvalidProtocolBufferException.class, () -> getProtoFromJson(json, Message.class));

    assertThat(ex)
        .hasMessageThat()
        .isEqualTo("Message type 'M' does not implement method 'M.Builder newBuilder()'.");
    assertThat(ex).hasCauseThat().isInstanceOf(java.io.IOException.class);
    assertThat(ex)
        .hasCauseThat()
        .hasCauseThat()
        .isInstanceOf(java.lang.ReflectiveOperationException.class);
  }
}
