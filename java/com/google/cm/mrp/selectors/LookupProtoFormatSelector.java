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

package com.google.cm.mrp.selectors;

import com.google.cm.lookupserver.api.LookupProto.LookupRequest;
import com.google.cm.lookupserver.api.LookupProto.LookupResponse;
import com.google.cm.shared.api.errors.ErrorResponseProto.ErrorResponse;
import com.google.cm.util.ProtoUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.core5.http.ContentType;

/** Enumerates objects for handling different types of lookup request encoding and decoding. */
public enum LookupProtoFormatSelector {
  /** Handles lookup requests/responses using JSON. */
  JSON(
      new LookupProtoFormatHandler() {
        @Override
        public ContentType getContentType() {
          return ContentType.APPLICATION_JSON;
        }

        @Override
        public byte[] getContentBytes(LookupRequest request) throws IOException {
          return ProtoUtils.getJsonFromProto(request).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public LookupResponse getLookupResponse(SimpleHttpResponse response) throws IOException {
          return ProtoUtils.getProtoFromJson(response.getBodyText(), LookupResponse.class);
        }

        @Override
        public ErrorResponse getErrorResponse(SimpleHttpResponse response) throws IOException {
          return ProtoUtils.getProtoFromJson(response.getBodyText(), ErrorResponse.class);
        }
      }),
  /** Handles lookup requests/responses using the protocol buffer binary format. */
  BINARY(
      new LookupProtoFormatHandler() {
        @Override
        public ContentType getContentType() {
          return ContentType.APPLICATION_OCTET_STREAM;
        }

        @Override
        public byte[] getContentBytes(LookupRequest request) throws IOException {
          try {
            return request.toByteArray();
          } catch (RuntimeException ex) {
            throw new IOException(ex);
          }
        }

        @Override
        public LookupResponse getLookupResponse(SimpleHttpResponse response) throws IOException {
          try {
            return response.getBody() == null
                ? LookupResponse.getDefaultInstance()
                : LookupResponse.parseFrom(response.getBodyBytes());
          } catch (RuntimeException ex) {
            throw new IOException(ex);
          }
        }

        @Override
        public ErrorResponse getErrorResponse(SimpleHttpResponse response) throws IOException {
          try {
            return response.getBody() == null
                ? ErrorResponse.getDefaultInstance()
                : ErrorResponse.parseFrom(response.getBodyBytes());
          } catch (RuntimeException ex) {
            throw new IOException(ex);
          }
        }
      });

  private final LookupProtoFormatHandler handler;

  LookupProtoFormatSelector(LookupProtoFormatHandler handler) {
    this.handler = handler;
  }

  public LookupProtoFormatHandler getFormatHandler() {
    return handler;
  }

  /**
   * Interface for objects that can convert lookup requests and responses to and from a particular
   * format type.
   */
  public interface LookupProtoFormatHandler {
    /** Returns the content type object corresponding to the request/response format. */
    ContentType getContentType();

    /** Encodes the message as bytes. */
    byte[] getContentBytes(LookupRequest request) throws IOException;

    /** Decodes the successful message from the response object. */
    LookupResponse getLookupResponse(SimpleHttpResponse response) throws IOException;

    /** Decodes the error message from the response object. */
    ErrorResponse getErrorResponse(SimpleHttpResponse response) throws IOException;
  }
}
