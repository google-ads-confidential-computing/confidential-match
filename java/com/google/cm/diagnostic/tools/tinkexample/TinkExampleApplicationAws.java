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

package com.google.cm.diagnostic.tools.tinkexample;

import static com.google.crypto.tink.aead.XChaCha20Poly1305Parameters.Variant.TINK;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNullElse;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.crypto.tink.Aead;
import com.google.crypto.tink.BinaryKeysetWriter;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.aead.AeadConfig;
import com.google.crypto.tink.aead.XChaCha20Poly1305Parameters;
import com.google.crypto.tink.integration.awskmsv2.AwsKmsV2Client;
import com.google.crypto.tink.proto.EncryptedKeyset;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;

public final class TinkExampleApplicationAws {

  private static final byte[] ASSOCIATED_DATA = new byte[0];
  private static final CSVFormat CSV_READ_FORMAT =
      CSVFormat.newFormat(',')
          .builder()
          .setRecordSeparator('\n')
          .setHeader()
          .setSkipHeaderRecord(true)
          .build();
  private static final CSVFormat CSV_WRITE_FORMAT_TEMPLATE =
      CSVFormat.newFormat(',').builder().setRecordSeparator('\n').build();
  private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();
  private static final MessageDigest SHA256_HASHER = getSha256Hasher();
  private static final Pattern PREFIX_PATTERN = Pattern.compile("^(?:mr|mrs|ms|dr)\\.\\s+");
  private static final Pattern SUFFIX_PATTERN =
      Pattern.compile("\\s+(?:jr\\.|sr\\.|2nd|3rd|ii|iii|iv|v|vi|cpa|dc|dds|vm|jd|md|phd)$");
  private static final Pattern NON_DIGITS_PATTERN = Pattern.compile("[^0-9]");
  private static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s");

  private TinkExampleApplicationAws() {}

  private static AwsKmsV2Client buildAwsV2Client() {
    try {
      var httpClient = ApacheHttpClient.builder().build();
      var credentialsProvider = ProfileCredentialsProvider.create();
      return new AwsKmsV2Client()
          .withHttpClient(httpClient)
          .withCredentialsProvider(credentialsProvider);
    } catch (GeneralSecurityException e) {
      throw new AssertionError("Failed to build AwsKmsV2Client", e);
    }
  }

  /**
   * Tink Example Application, prints input CSV records to a new file with columns that are
   * normalized and/or encrypted as needed.
   *
   * @param args Application CLI arguments, see {@link TinkExampleApplicationArguments} below
   * @throws IOException when there is an error reading or writing files
   * @throws GeneralSecurityException when there is an encryption-related error
   */
  public static void main(String[] args) throws IOException, GeneralSecurityException {

    // Set variables based on arguments.
    TinkExampleApplicationArguments cliArgs = TinkExampleApplicationArguments.parseMainArgs(args);
    String kekUri = cliArgs.kekUri;
    Map<String, String> columnTypes = cliArgs.columnTypes;
    Path inputFilePath = Path.of(cliArgs.inputFilePath);
    Path outputFilePath = Path.of(cliArgs.outputFilePath);

    // Enable Tink's AEAD handling.
    AeadConfig.register();

    AwsKmsV2Client kmsV2Client = buildAwsV2Client();
    // Get the Key Encryption Key (KEK) keyset handle and its AEAD.
    Aead kekAead = kmsV2Client.getAead(kekUri);

    // Create a Data Encryption Key (DEK) keyset, then get its handle and AEAD.
    KeysetHandle dekHandle = KeysetHandle.generateNew(XChaCha20Poly1305Parameters.create(TINK));
    Aead dekAead = dekHandle.getPrimitive(Aead.class);

    // Get the encrypted DEK keyset as a Base64 ciphertext, for printing as output.
    ByteArrayOutputStream dekStream = new ByteArrayOutputStream();
    dekHandle.write(BinaryKeysetWriter.withOutputStream(dekStream), kekAead);
    var dekBytes = EncryptedKeyset.parseFrom(dekStream.toByteArray()).getEncryptedKeyset();
    String encryptedDek = BASE64_ENCODER.encodeToString(dekBytes.toByteArray());

    // Generate a file with encrypted records.
    try (BufferedReader reader = Files.newBufferedReader(inputFilePath);
        CSVParser parser = CSV_READ_FORMAT.parse(reader);
        BufferedWriter writer = Files.newBufferedWriter(outputFilePath);
        CSVPrinter printer = getCsvWriteFormat(parser.getHeaderNames()).print(writer)) {
      try {

        // Iterate over every record in the input CSV file.
        for (CSVRecord record : parser) {

          // Verify that the record contains all header columns.
          if (!record.isConsistent()) {
            throw new IllegalArgumentException("Inconsistent CSV record cannot be encrypted.");
          }

          // Print column value after performing needed normalization and/or encryption.
          for (String column : parser.getHeaderNames()) {
            String text = record.get(column);
            String dataType = requireNonNullElse(columnTypes.get(column), "unknown");
            printer.print(processColumn(text, dataType, dekAead));
          }

          // Print the encrypted DEK column, KEK URI column, and end the record line.
          printer.print(encryptedDek);
          printer.print(kekUri);
          printer.println();
        }
      } catch (IOException | GeneralSecurityException | RuntimeException ex) {

        // When an exception is thrown, include the CSV parser's current line number.
        throw new IOException("Encryption failed on line " + parser.getCurrentLineNumber(), ex);
      }
    }
  }

  private static MessageDigest getSha256Hasher() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException ex) {
      throw new RuntimeException(ex);
    }
  }

  private static CSVFormat getCsvWriteFormat(List<String> headerNames) {

    // Add KEK URI and encrypted DEK to the input file's header names.
    String[] headerRecord = new String[headerNames.size() + 2];
    headerNames.toArray(headerRecord);
    headerRecord[headerRecord.length - 2] = "encrypted_dek";
    headerRecord[headerRecord.length - 1] = "kek_uri";

    // Add headers to the CSV writer format.
    return CSV_WRITE_FORMAT_TEMPLATE.builder().setHeader(headerRecord).build();
  }

  private static String processColumn(String input, String dataType, Aead dekAead)
      throws GeneralSecurityException {

    // Skip processing if there is no text.
    if (input.isEmpty()) {
      return input;
    }

    // Normalize and/or encrypt columns as needed.
    switch (dataType.toLowerCase(Locale.US)) {
      case "email":
        return encrypt(dekAead, hash(normalizeEmail(input)));
      case "phone":
        return encrypt(dekAead, hash(normalizePhone(input)));
      case "first_name":
        return encrypt(dekAead, hash(normalizeFirstName(input)));
      case "last_name":
        return encrypt(dekAead, hash(normalizeLastName(input)));
      case "country_code":
      // fallthrough
      case "zip_code":
      // fallthrough
      default:
        // No changes
        return input;
    }
  }

  private static String normalizeEmail(String input) {

    // Make lowercase and remove all whitespace.
    return WHITESPACE_PATTERN.matcher(input.toLowerCase(Locale.US)).replaceAll("");
  }

  private static String normalizeFirstName(String input) {

    // Remove prefix (e.g. "Mr."), trim whitespace from both ends, and make lowercase.
    return PREFIX_PATTERN.matcher(input.strip().toLowerCase(Locale.US)).replaceAll("");
  }

  private static String normalizeLastName(String input) {

    // Remove suffix (e.g. "Jr."), trim whitespace from both ends, and make lowercase.
    return SUFFIX_PATTERN.matcher(input.strip().toLowerCase(Locale.US)).replaceAll("");
  }

  private static String normalizePhone(String input) {

    // Remove non-digit characters, but always have a leading '+' character.
    return "+" + NON_DIGITS_PATTERN.matcher(input).replaceAll("");
  }

  private static String hash(String text) {

    // Hash the text and encode it in Base64.
    return BASE64_ENCODER.encodeToString(SHA256_HASHER.digest(text.getBytes(UTF_8)));
  }

  private static String encrypt(Aead dekAead, String text) throws GeneralSecurityException {

    // Encrypt the text and encode it in Base64.
    return BASE64_ENCODER.encodeToString(dekAead.encrypt(text.getBytes(UTF_8), ASSOCIATED_DATA));
  }

  /** Class for parsing command line arguments. */
  private static class TinkExampleApplicationArguments {

    private TinkExampleApplicationArguments() {}

    static TinkExampleApplicationArguments parseMainArgs(String[] args) {
      TinkExampleApplicationArguments cliArgs = new TinkExampleApplicationArguments();
      try {
        JCommander.newBuilder().addObject(cliArgs).build().parse(args);
      } catch (ParameterException ex) {
        ex.getJCommander().usage();
        throw ex;
      }
      return cliArgs;
    }

    @Parameter(
        names = {"-i", "--input-file"},
        required = true,
        description = "Input CSV file. Must contain a header record. Values must be cleartext.")
    String inputFilePath;

    @Parameter(
        names = {"-o", "--output-file"},
        required = true,
        description = "Output CSV file.")
    String outputFilePath;

    @Parameter(
        names = {"-u", "--kek-uri"},
        required = true,
        description =
            "AWS KMS KEK URI in the format "
                + "\"arn:aws:kms:{{region}}:{{account-id}}:key/{{UUID}}\"\".")
    String kekUri;

    @DynamicParameter(
        names = {"-t", "--column-type"},
        description =
            "This flag requires two arguments separated with an equal sign: "
                + "first the name of a column, then its data type. "
                + "Use a separate flag for each pair. Example: -t Email=email -t Phone=phone "
                + "Valid types: email, phone, first_name, last_name, country_code, and zip_code. "
                + "Includes by default one column for each type, where each name matches the type.")
    Map<String, String> columnTypes =
        new HashMap<>(
            Map.ofEntries(
                Map.entry("email", "email"),
                Map.entry("phone", "phone"),
                Map.entry("first_name", "first_name"),
                Map.entry("last_name", "last_name"),
                Map.entry("country_code", "country_code"),
                Map.entry("zip_code", "zip_code")));
  }
}
