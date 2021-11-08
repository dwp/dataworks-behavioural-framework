import base64
import pathlib

import binascii
from Crypto import Random
from Crypto.Cipher import AES
from Crypto.Util import Counter


# Encrypt the data using AES CTR algorithm
def encrypt_data_aes_ctr(data_key, plaintext_string, iv=None):
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

    return ciphertext.decode("ascii"), iv.decode("ascii")


def read_test_data(file_name):
    home_dir = pathlib.Path(__file__).resolve().parents[2]
    test_data_location = (
        "src/fixture-data/functional-tests/dataworks_kafka_producer_data"
    )
    file_path = f"{home_dir}/{test_data_location}/{file_name}"
    fh = open(file_path, "r")
    plaintext_string = fh.read()
    return plaintext_string
