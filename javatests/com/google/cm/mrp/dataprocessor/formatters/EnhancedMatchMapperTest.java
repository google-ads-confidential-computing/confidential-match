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

import static com.google.common.hash.Hashing.sha256;
import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertThrows;

import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord.KeyValue;
import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class EnhancedMatchMapperTest {

  private static final Hashes EMAIL_HASH = new Hashes("example@example.com");
  private static final Hashes PHONE_NUMBER_HASH = new Hashes("+15555555555");
  private static final Hashes FIRST_NAME_HASH = new Hashes("Lorem");
  private static final Hashes LAST_NAME_HASH = new Hashes("Ipsum");
  private static final String POSTAL_CODE = "12345";
  private static final String COUNTRY = "US";

  @Test
  public void mapGtagToDataRecord_email_returns() {
    DataRecord record =
        EnhancedMatchMapper.mapGtagToDataRecord(
            String.format("tv.1~em.%s", EMAIL_HASH.getWebHashBase64()));

    assertThat(record.getKeyValuesList())
        .containsExactly(
            KeyValue.newBuilder()
                .setKey("em")
                .setStringValue(EMAIL_HASH.getCfmHashBase64())
                .build());
  }

  @Test
  public void mapGtagToDataRecord_emailHex_returns() {
    DataRecord record =
        EnhancedMatchMapper.mapGtagToDataRecord(
            String.format("tv.1~em.%s", EMAIL_HASH.getWebHashHex()));

    assertThat(record.getKeyValuesList())
        .containsExactly(
            KeyValue.newBuilder()
                .setKey("em")
                .setStringValue(EMAIL_HASH.getCfmHashBase64())
                .build());
  }

  @Test
  public void mapGtagToDataRecord_multipleEmail_returns() {
    Hashes email0 = EMAIL_HASH;
    Hashes email1 = new Hashes("1" + EMAIL_HASH.toString().substring(1));
    Hashes email2 = new Hashes("2" + EMAIL_HASH.toString().substring(1));
    Hashes email3 = new Hashes("3" + EMAIL_HASH.toString().substring(1));

    DataRecord record =
        EnhancedMatchMapper.mapGtagToDataRecord(
            String.format(
                "tv.1~em.%s~em.%s~em.%s~em.%s",
                email0.getWebHashBase64(),
                email1.getWebHashBase64(),
                email2.getWebHashBase64(),
                email3.getWebHashBase64()));

    assertThat(record.getKeyValuesList())
        .containsExactly(
            KeyValue.newBuilder().setKey("em").setStringValue(email0.getCfmHashBase64()).build(),
            KeyValue.newBuilder().setKey("em").setStringValue(email1.getCfmHashBase64()).build(),
            KeyValue.newBuilder().setKey("em").setStringValue(email2.getCfmHashBase64()).build(),
            KeyValue.newBuilder().setKey("em").setStringValue(email3.getCfmHashBase64()).build())
        .inOrder();
  }

  @Test
  public void mapGtagToDataRecord_emailIncludesError_returns() {
    Hashes email0 = EMAIL_HASH;
    String email1 = "e2"; // error
    String email2 = "e23"; // error
    String email3 = "bad hash";
    Hashes email4 = new Hashes("4" + EMAIL_HASH.toString().substring(1));

    DataRecord record =
        EnhancedMatchMapper.mapGtagToDataRecord(
            String.format(
                "tv.1~em.%s~em.%s~em.%s~em.%s~em.%s",
                email0.getWebHashBase64(), email1, email2, email3, email4.getWebHashBase64()));

    assertThat(record.getKeyValuesList())
        .containsExactly(
            KeyValue.newBuilder().setKey("em").setStringValue(email0.getCfmHashBase64()).build(),
            KeyValue.newBuilder().setKey("em").setStringValue("bad hash").build(),
            KeyValue.newBuilder().setKey("em").setStringValue(email4.getCfmHashBase64()).build(),
            KeyValue.newBuilder().setKey("error_codes").setStringValue("em.e2~em.e23").build())
        .inOrder();
  }

  @Test
  public void mapGtagToDataRecord_phoneNumber_returns() {
    DataRecord record =
        EnhancedMatchMapper.mapGtagToDataRecord(
            String.format("tv.1~pn.%s", PHONE_NUMBER_HASH.getWebHashBase64()));

    assertThat(record.getKeyValuesList())
        .containsExactly(
            KeyValue.newBuilder()
                .setKey("pn")
                .setStringValue(PHONE_NUMBER_HASH.getCfmHashBase64())
                .build());
  }

  @Test
  public void mapGtagToDataRecord_phoneNumberHex_returns() {
    DataRecord record =
        EnhancedMatchMapper.mapGtagToDataRecord(
            String.format("tv.1~pn.%s", PHONE_NUMBER_HASH.getWebHashHex()));

    assertThat(record.getKeyValuesList())
        .containsExactly(
            KeyValue.newBuilder()
                .setKey("pn")
                .setStringValue(PHONE_NUMBER_HASH.getCfmHashBase64())
                .build());
  }

  @Test
  public void mapGtagToDataRecord_multiplePhoneNumbers_returns() {
    Hashes phoneNumber0 = PHONE_NUMBER_HASH;
    Hashes phoneNumber1 = new Hashes("1" + PHONE_NUMBER_HASH.toString().substring(1));
    Hashes phoneNumber2 = new Hashes("2" + PHONE_NUMBER_HASH.toString().substring(1));
    Hashes phoneNumber3 = new Hashes("3" + PHONE_NUMBER_HASH.toString().substring(1));

    DataRecord record =
        EnhancedMatchMapper.mapGtagToDataRecord(
            String.format(
                "tv.1~pn.%s~pn.%s~pn.%s~pn.%s",
                phoneNumber0.getWebHashBase64(),
                phoneNumber1.getWebHashBase64(),
                phoneNumber2.getWebHashBase64(),
                phoneNumber3.getWebHashBase64()));

    assertThat(record.getKeyValuesList())
        .containsExactly(
            KeyValue.newBuilder()
                .setKey("pn")
                .setStringValue(phoneNumber0.getCfmHashBase64())
                .build(),
            KeyValue.newBuilder()
                .setKey("pn")
                .setStringValue(phoneNumber1.getCfmHashBase64())
                .build(),
            KeyValue.newBuilder()
                .setKey("pn")
                .setStringValue(phoneNumber2.getCfmHashBase64())
                .build(),
            KeyValue.newBuilder()
                .setKey("pn")
                .setStringValue(phoneNumber3.getCfmHashBase64())
                .build())
        .inOrder();
  }

  @Test
  public void mapGtagToDataRecord_phoneNumberIncludesError_returns() {
    Hashes phoneNumber0 = PHONE_NUMBER_HASH;
    String phoneNumber1 = "e2"; // error
    String phoneNumber2 = "e23"; // error
    String phoneNumber3 = "bad hash";
    Hashes phoneNumber4 = new Hashes("4" + PHONE_NUMBER_HASH.toString().substring(1));

    DataRecord record =
        EnhancedMatchMapper.mapGtagToDataRecord(
            String.format(
                "tv.1~pn.%s~pn.%s~pn.%s~pn.%s~pn.%s",
                phoneNumber0.getWebHashBase64(),
                phoneNumber1,
                phoneNumber2,
                phoneNumber3,
                phoneNumber4.getWebHashBase64()));

    assertThat(record.getKeyValuesList())
        .containsExactly(
            KeyValue.newBuilder()
                .setKey("pn")
                .setStringValue(phoneNumber0.getCfmHashBase64())
                .build(),
            KeyValue.newBuilder().setKey("pn").setStringValue("bad hash").build(),
            KeyValue.newBuilder()
                .setKey("pn")
                .setStringValue(phoneNumber4.getCfmHashBase64())
                .build(),
            KeyValue.newBuilder().setKey("error_codes").setStringValue("pn.e2~pn.e23").build())
        .inOrder();
  }

  @Test
  public void mapGtagToDataRecord_address_returns() {
    DataRecord record =
        EnhancedMatchMapper.mapGtagToDataRecord(
            String.format(
                "tv.1~fn0.%s~ln0.%s~pc0.%s~co0.%s",
                FIRST_NAME_HASH.getWebHashBase64(),
                LAST_NAME_HASH.getWebHashBase64(),
                POSTAL_CODE,
                COUNTRY.toUpperCase()));

    assertThat(record.getKeyValuesList())
        .containsExactly(
            KeyValue.newBuilder()
                .setKey("fn0")
                .setStringValue(FIRST_NAME_HASH.getCfmHashBase64())
                .build(),
            KeyValue.newBuilder()
                .setKey("ln0")
                .setStringValue(LAST_NAME_HASH.getCfmHashBase64())
                .build(),
            KeyValue.newBuilder().setKey("pc0").setStringValue(POSTAL_CODE.toLowerCase()).build(),
            KeyValue.newBuilder().setKey("co0").setStringValue(COUNTRY.toLowerCase()).build())
        .inOrder();
  }

  @Test
  public void mapGtagToDataRecord_address_returnsPartial() {
    DataRecord record =
        EnhancedMatchMapper.mapGtagToDataRecord(
            String.format(
                "tv.1~fn0.%s~ln0.%s~co0.%s",
                FIRST_NAME_HASH.getWebHashHex(),
                LAST_NAME_HASH.getWebHashHex(),
                COUNTRY.toUpperCase()));

    assertThat(record.getKeyValuesList())
        .containsExactly(
            KeyValue.newBuilder()
                .setKey("fn0")
                .setStringValue(FIRST_NAME_HASH.getCfmHashBase64())
                .build(),
            KeyValue.newBuilder()
                .setKey("ln0")
                .setStringValue(LAST_NAME_HASH.getCfmHashBase64())
                .build(),
            KeyValue.newBuilder().setKey("co0").setStringValue(COUNTRY.toLowerCase()).build())
        .inOrder();
  }

  @Test
  public void mapGtagToDataRecord_address_returnsMultiple() {
    Hashes firstNameHash1 = new Hashes("Foo");
    Hashes lastNameHash1 = new Hashes("Bar");
    String postalCode1 = "98765";
    String country1 = "DE";
    Hashes firstNameHash2 = new Hashes("Baz");
    String country2 = "FR";
    String country4 = "CA";

    DataRecord record =
        EnhancedMatchMapper.mapGtagToDataRecord(
            String.format(
                "tv.1~fn0.%s~ln0.%s~pc0.%s~co0.%s~fn1.%s~ln1.%s~pc1.%s~co1.%s~fn2.%s~co2.%s~co4.%s",
                FIRST_NAME_HASH.getWebHashBase64(),
                LAST_NAME_HASH.getWebHashBase64(),
                POSTAL_CODE,
                COUNTRY,
                firstNameHash1.getCfmHashBase64(),
                lastNameHash1.getCfmHashBase64(),
                postalCode1.toLowerCase(),
                country1.toLowerCase(),
                firstNameHash2.getCfmHashBase64(),
                country2.toLowerCase(),
                country4.toLowerCase()));

    assertThat(record.getKeyValuesList())
        .containsExactly(
            KeyValue.newBuilder()
                .setKey("fn0")
                .setStringValue(FIRST_NAME_HASH.getCfmHashBase64())
                .build(),
            KeyValue.newBuilder()
                .setKey("ln0")
                .setStringValue(LAST_NAME_HASH.getCfmHashBase64())
                .build(),
            KeyValue.newBuilder().setKey("pc0").setStringValue(POSTAL_CODE).build(),
            KeyValue.newBuilder().setKey("co0").setStringValue(COUNTRY.toLowerCase()).build(),
            KeyValue.newBuilder()
                .setKey("fn1")
                .setStringValue(firstNameHash1.getCfmHashBase64())
                .build(),
            KeyValue.newBuilder()
                .setKey("ln1")
                .setStringValue(lastNameHash1.getCfmHashBase64())
                .build(),
            KeyValue.newBuilder().setKey("pc1").setStringValue(postalCode1).build(),
            KeyValue.newBuilder().setKey("co1").setStringValue(country1.toLowerCase()).build(),
            KeyValue.newBuilder()
                .setKey("fn2")
                .setStringValue(firstNameHash2.getCfmHashBase64())
                .build(),
            KeyValue.newBuilder().setKey("co2").setStringValue(country2.toLowerCase()).build(),
            KeyValue.newBuilder().setKey("co4").setStringValue(country4.toLowerCase()).build())
        .inOrder();
  }

  @Test
  public void mapGtagToDataRecord_addressIncludesError_returns() {
    String firstName = "e2";
    String lastName = "bad hash";
    DataRecord record =
        EnhancedMatchMapper.mapGtagToDataRecord(
            String.format("tv.1~fn0.%s~ln0.%s", firstName, lastName));

    assertThat(record.getKeyValuesList())
        .containsExactly(
            KeyValue.newBuilder().setKey("ln0").setStringValue(lastName).build(),
            KeyValue.newBuilder().setKey("error_codes").setStringValue("fn0.e2").build())
        .inOrder();
  }

  @Test
  public void mapGtagToDataRecord_protoIfNoFieldsFound_empty() {
    DataRecord record = EnhancedMatchMapper.mapGtagToDataRecord("tv.1");

    assertThat(record.getKeyValuesList()).isEmpty();
  }

  @Test
  public void mapGtagToDataRecord_invalidTokens_ignores() {
    ImmutableList<String> badTokens =
        ImmutableList.of("foo", "foo.", ".foo", ".", "~", "....", ".foo.bar.", "fake.value");

    for (String badToken : badTokens) {
      DataRecord record =
          EnhancedMatchMapper.mapGtagToDataRecord(
              String.format(
                  "tv.1~em.%s~%s~pn.%s",
                  EMAIL_HASH.getWebHashBase64(), badToken, PHONE_NUMBER_HASH.getWebHashBase64()));

      assertThat(record.getKeyValuesList())
          .containsExactly(
              KeyValue.newBuilder()
                  .setKey("em")
                  .setStringValue(EMAIL_HASH.getCfmHashBase64())
                  .build(),
              KeyValue.newBuilder()
                  .setKey("pn")
                  .setStringValue(PHONE_NUMBER_HASH.getCfmHashBase64())
                  .build())
          .inOrder();
    }
  }

  @Test
  public void mapGtagToDataRecord_nullInput_throws() {
    Exception e =
        assertThrows(
            IllegalArgumentException.class, () -> EnhancedMatchMapper.mapGtagToDataRecord(null));

    assertThat(e).hasMessageThat().contains("null");
  }

  @Test
  public void mapGtagToDataRecord_longInput_throws() {
    StringBuffer superLongInput = new StringBuffer();
    for (int i = 0; i < 1000; i++) {
      superLongInput.append(EMAIL_HASH.getWebHashBase64());
    }

    Exception e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                EnhancedMatchMapper.mapGtagToDataRecord(
                    String.format("tv.1~em.%s", superLongInput.toString())));

    assertThat(e).hasMessageThat().contains("exceed");
  }

  @Test
  public void mapGtagToDataRecord_tagVersionMissing_throws() {
    Exception e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                EnhancedMatchMapper.mapGtagToDataRecord(
                    String.format("em.%s", EMAIL_HASH.getWebHashBase64())));

    assertThat(e).hasMessageThat().contains("tag version");
  }

  @Test
  public void mapGtagToDataRecord_tagVersionIncorrect_throws() {
    Exception e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                EnhancedMatchMapper.mapGtagToDataRecord(
                    String.format("tv.2~em.%s", EMAIL_HASH.getWebHashBase64())));

    assertThat(e).hasMessageThat().contains("tag version");
  }

  @Test
  public void mapGtagToDataRecord_errorCode_returns() {
    DataRecord record = EnhancedMatchMapper.mapGtagToDataRecord("tv.1~ec.e3");

    assertThat(record.getKeyValuesList())
        .containsExactly(
            KeyValue.newBuilder().setKey("error_codes").setStringValue("ec.e3").build());
  }

  @Test
  public void hashesClass_hashesInput_returns() {
    Hashes email = new Hashes("example@example.com");
    assertThat(email.getWebHashBase64()).isEqualTo("McVUPBc00lxyBvX9WRUl0Clb7G_oT_gvlGo0_pcKHmY");
    assertThat(email.getWebHashHex())
        .isEqualTo("31c5543c1734d25c7206f5fd591525d0295bec6fe84ff82f946a34fe970a1e66");
    assertThat(email.getCfmHashBase64()).isEqualTo("McVUPBc00lxyBvX9WRUl0Clb7G/oT/gvlGo0/pcKHmY=");
  }

  public static class Hashes {

    final String input;
    final String webHashBase64;
    final String webHashHex;
    final String cfmHashBase64;

    public Hashes(String input) {
      this.input = input;
      // Url-safe Base64, with padding removed
      this.webHashBase64 =
          BaseEncoding.base64Url()
              .encode(sha256().hashBytes(input.getBytes(UTF_8)).asBytes())
              .substring(0, 43);
      this.webHashHex =
          BaseEncoding.base16()
              .encode(sha256().hashBytes(input.getBytes(UTF_8)).asBytes())
              .toLowerCase();
      this.cfmHashBase64 =
          BaseEncoding.base64().encode(sha256().hashBytes(input.getBytes(UTF_8)).asBytes());
    }

    public String getWebHashBase64() {
      return webHashBase64;
    }

    String getWebHashHex() {
      return webHashHex;
    }

    public String getCfmHashBase64() {
      return cfmHashBase64;
    }

    public String toString() {
      return input;
    }
  }
}
