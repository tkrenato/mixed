"""
 * Copyright (C) 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *"""

# This code has the objective of generate encrypted user, password and connection url
# to be used in a Dataflow job.

# Usage
# $ python encrypt_text_kms.py project_id location_id key_ring crypto_key text_to_be_converted

import base64
from google.cloud import kms
import argparse


def encrypt_symmetric(project_id, location_id, key_ring, crypto_key, plaintext):
    password = bytes(plaintext, "utf-8")
    encoded_password = base64.b64encode(password).decode(
        "utf-8"
    )  # Encode the password as base64

    # Create the client.
    client = kms.KeyManagementServiceClient()

    key_name = client.crypto_key_path(project_id, location_id, key_ring, crypto_key)

    # Call the API to encrypt the password.
    response = client.encrypt(request={"name": key_name, "plaintext": encoded_password})

    ciphertext = response.ciphertext
    ciphertext_base64 = base64.b64encode(ciphertext).decode("utf-8")
    encrypted_password = "base64:{}".format(ciphertext_base64)
    print("Encrypted Password: {}".format(encrypted_password))
    return encrypted_password


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("project_id", help="id of the GCP project")
    parser.add_argument("location_id", help="id of the KMS location eg: global")
    parser.add_argument("key_ring", help="Key Ring id")
    parser.add_argument("crypto_key", help="Crypto Key id")
    parser.add_argument("plaintext", help="Text to be encrypted")
    args = parser.parse_args()

    encrypt_symmetric(
        args.project_id,
        args.location_id,
        args.key_ring,
        args.crypto_key,
        args.plaintext,
    )
