import base64
import os
import pathlib

import binascii
from Crypto import Random
from Crypto.Cipher import AES, PKCS1_OAEP
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from Crypto.Random import get_random_bytes
from Crypto.Util import Counter

from helpers import aws_helper, console_printer


def get_hsm_pub_key_id(p_hsm_key_id):
    pub_key_id = aws_helper.get_ssm_parameter_value(
        p_hsm_key_id
    )
    return pub_key_id


def get_hsm_pub_key(p_hsm_pub_key):
    hsm_pub_key = aws_helper.get_ssm_parameter_value(
        p_hsm_pub_key
    )
    return hsm_pub_key


def create_cipher(hsm_pub_key):
    hsm_key = RSA.import_key(hsm_pub_key)
    return PKCS1_OAEP.new(key=hsm_key, hashAlgo=SHA256)


def create_encrypted_data_key(cipher_rsa, data_key):
    return cipher_rsa.encrypt(data_key)


def create_new_data_key(context):
    # Generate a random data key
    data_key = get_random_bytes(16)
    # Get the public key from SSM
    hsm_pub_key = base64.b64decode(get_hsm_pub_key(context.dataworks_streams_kafka_producer_hsm_pub_key))
    # Create an encrypted data key using public key and data key
    cipher_rsa = create_cipher(hsm_pub_key)
    enc_data_key = create_encrypted_data_key(cipher_rsa, data_key)
    return data_key, hsm_pub_key, base64.b64encode(enc_data_key)


# Encrypt the data using AES CTR algorithm
def encrypt_data_aes_ctr(context, data_key, plaintext_string, iv=None):
    if iv is None:
        initialisation_vector = Random.new().read(AES.block_size)
        iv_int = int(binascii.hexlify(initialisation_vector), 16)
        iv = base64.b64encode(initialisation_vector)
    else:
        initialisation_vector = base64.b64decode(iv.encode("ascii"))
        iv_int = int(binascii.hexlify(initialisation_vector), 16)
        iv = base64.b64encode(initialisation_vector)

    counter = Counter.new(AES.block_size * 8, initial_value=iv_int)
    aes = AES.new(data_key, AES.MODE_CTR, counter=counter)
    ciphertext = aes.encrypt(plaintext_string.encode("utf8"))
    ciphertext = base64.b64encode(ciphertext)
    encryption_key_id = get_hsm_pub_key_id(context.dataworks_streams_kafka_producer_hsm_key_id)
    return ciphertext.decode("ascii"), iv.decode("ascii"), encryption_key_id


def read_test_data(file_name):
    home_dir = pathlib.Path(__file__).resolve().parents[2]
    test_data_location = (
        "src/fixture-data/functional-tests/dataworks_kafka_producer_data"
    )
    file_path = f"{home_dir}/{test_data_location}/{file_name}"
    fh = open(file_path, "r")
    plaintext_string = fh.read()
    return plaintext_string
