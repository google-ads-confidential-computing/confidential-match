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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;

/** Parsing utility for protocol buffers. */
public final class ProtoUtils {

  private ProtoUtils() {
  }

  /** Prints a protocol buffer into a JSON string. */
  public static String getJsonFromProto(MessageOrBuilder proto)
      throws InvalidProtocolBufferException {
    return JsonFormat.printer()
        .alwaysPrintFieldsWithNoPresence()
        .omittingInsignificantWhitespace()
        .print(proto);
  }

  /** Converts a JSON string into the given protocol buffer. */
  public static <M extends Message> M getProtoFromJson(String json, Class<M> messageType)
      throws InvalidProtocolBufferException {
    Message.Builder builder;
    try {
      builder = (Message.Builder) messageType.getMethod("newBuilder").invoke(null);
    } catch (ReflectiveOperationException ex) {
      var cause = new IOException(ex);
      throw new InvalidProtocolBufferException(
          "Message type 'M' does not implement method 'M.Builder newBuilder()'.", cause);
    }

    JsonFormat.parser().ignoringUnknownFields().merge(json, builder);
    return messageType.cast(builder.build());
  }
}
