# Encryption Application Example

This example application takes a CSV file as input and produces a formatted and
encrypted CSV file. The application accepts plaintext data files, and then
it [formats](https://support.google.com/google-ads/answer/7659867), hashes, and
encrypts this data with an encrypted keyset using Tink version 1.7. For more
information and examples,
see [Create and store an encrypted keyset](https://developers.google.com/tink/generate-encrypted-keyset)
and [EncryptedKeysetExample](https://github.com/tink-crypto/tink/blob/1.7/java_src/examples/encryptedkeyset/EncryptedKeysetExample.java).

If you are using a database or CRM system, you will need to take additional
steps to encrypt your data.

## Prerequisites

* A Key Encryption Key (KEK) URI from Google Cloud KMS.
  See [Manage Keys | Tink](https://developers.google.com/tink/key-management-overview).
* Supply the Google Cloud project credentials using the `-c` flag with a
  credentials file. If they are not provided, the application attempts to use
  glcoud default credentials.
  See [Set up Application Default Credentials](https://cloud.google.com/docs/authentication/provide-credentials-adc).
* The input data CSV file must use these header columns:
  * email
  * phone
  * first_name
  * last_name
  * zip_code
  * country_code
* The credentials used to run this application must have the IAM role `Cloud KMS
  CryptoKey Encrypter/Decrypter` for the KEK.

## Usage

1. Get the URI for the KEK in a format that Tink can parse:

In the command below, replace the following variables:

* `KEY`: the name of the key.
* `KEY_RING`: the name of the key ring.
* `LOCATION`: the location of the key ring.

```bash
echo "gcp-kms://$(gcloud kms keys describe KEY --keyring=KEY_RING --location=LOCATION --format='value(name)')"
```

2. Run the application to encrypt your data:

In the command below, replace the following variables:

* `-i`: the input CSV file name, including the extension.
* `-o`: the output CSV file name, including the extension.
* `-u`: the KEK URI.

```bash
/usr/bin/java -jar tink-example-application.jar \
-i INPUT_FILE -o OUTPUT_FILE \
-u 'gcp-kms://projects/PROJECT/locations/LOCATION/keyRings/KEY_RING/cryptoKeys/KEY'
```

Example:

```bash
/usr/bin/java -jar tink-example-application.jar -i input.csv -o output.csv -u 'gcp-kms://projects/project/locations/global/keyRings/customer-match-data-encryption-key-ring/cryptoKeys/customer-match-data-encryption-key'
```

See the `TinkExampleApplicationArguments` class definition
in `TinkExampleApplication.java` for a description of the arguments. If there is
an error with parsing the arguments, the application will display usage
information.

The output file will contain the initial header columns and two new columns:

* email
* phone
* first_name
* last_name
* zip_code
* country_code
* encrypted_dek
* kek_uri
