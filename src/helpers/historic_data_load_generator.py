import base64
import binascii
import gzip
import json
import uuid
import os
import time
import json
import threading
from traceback import print_exc
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime
from Crypto import Random
from Crypto.Cipher import AES
from Crypto.Util import Counter
from helpers import (
    file_helper,
    template_helper,
    date_helper,
    console_printer,
    aws_helper,
    historic_data_generator,
)

import_data_file_extension = historic_data_generator.import_data_file_extension
import_metadata_file_extension = historic_data_generator.import_metadata_file_extension

keys = [[uuid.uuid4(), "", ""]]
key_method = "single"
_base_datetime_timestamp = datetime.strptime(
    "2018-11-01T03:02:01.001", "%Y-%m-%dT%H:%M:%S.%f"
)
_keys_lock = threading.Lock()


def _generate_keys(file_count, record_count, key=None):
    """Sets up the keys depending on the desired method.

    Keyword arguments:
    file_count -- the number of files being generated
    record_count -- the number of records being generated in each file
    key -- static key to use or None to generate new key(s)
    """
    global keys
    global key_method

    keys.clear()

    console_printer.print_info(f"Globals key method set to {key_method}")

    if key_method == "different":
        keys = [
            [uuid.uuid4(), "", ""]
            for key_count in range(1, (file_count * record_count) + 1)
        ]
    elif key_method == "file":
        keys = [[uuid.uuid4(), "", ""] for key_count in range(1, file_count + 1)]
    elif key_method == "record":
        keys = [[uuid.uuid4(), "", ""] for key_count in range(1, record_count + 1)]
    else:
        key_method = "single"
        if key is not None:
            console_printer.print_info(
                f"Using single static key of {key} for all records"
            )
            key_qualified = key
        else:
            console_printer.print_info(f"Creating single static key for all records")
            key_qualified = uuid.uuid4()
        keys = [[key_qualified, "", ""]]


def generate_historic_load_data(
    s3_bucket,
    s3_prefix,
    method,
    file_count,
    record_count,
    topic,
    input_template,
    encrypted_key,
    plaintext_key,
    master_key,
    input_folder,
    max_worker_count,
    static_key=None,
):
    """Generates required historic data files from the files in the given folder.

    Keyword arguments:
    s3_bucket -- the s3 bucket to send input files to
    s3_prefix -- the s3 prefix to send input files to
    method -- 'single', 'different', 'file' or 'record' for the key method to use for this data
    file_count -- the number of files to create for the collection
    record_count -- the number of records per file
    topic -- the topic (contains db.collection)
    input_template -- the name and location for the input template json file
    encrypted_key -- the encrypted version of the plaintext key
    plaintext_key -- the plaintext data key for encrypting the data file
    master_key -- the master key used to encrypt the data key
    input_folder -- the folder to store the generated input files in
    static_key -- the key to use for the messages or None
    max_worker_count -- max thread number
    """
    global key_method
    key_method = method

    _generate_keys(file_count, record_count, static_key)

    console_printer.print_info(
        f"Generating {str(file_count)} load files with {str(record_count)} records in each using key method {key_method} for topic {topic}"
    )

    short_topic = template_helper.get_short_topic_name(topic)

    for result_input in _generate_input_data_files_threaded(
        s3_bucket,
        s3_prefix,
        file_count,
        record_count,
        short_topic,
        input_template,
        encrypted_key,
        plaintext_key,
        master_key,
        input_folder,
        max_worker_count,
    ):
        console_printer.print_info(f"Generated input file {result_input}")


def _generate_input_data_files_threaded(
    s3_bucket,
    s3_prefix,
    file_count,
    record_count,
    short_topic,
    input_template,
    encrypted_key,
    plaintext_key,
    master_key,
    input_folder,
    max_worker_count,
):
    """Generates required historic data files from the files in the given folder using multiple threads.

    Keyword arguments:
    s3_bucket -- the s3 bucket to send input files to
    s3_prefix -- the s3 prefix to send input files to
    file_count -- the number of files to create for the collection
    record_count -- the number of records per file
    short_topic -- the short topic
    input_template -- the name and location for the input template json file
    encrypted_key -- the encrypted version of the plaintext key
    plaintext_key -- the plaintext data key for encrypting the data file
    master_key -- the master key used to encrypt the data key
    input_folder -- the folder to store the generated input files in
    max_worker_count -- max thread number
    """
    input_base_content = file_helper.get_contents_of_file(input_template, False)
    [file_iv_int, file_iv_whole] = generate_initialisation_vector()
    encryption_json_text = generate_encryption_metadata_for_metadata_file(
        encrypted_key, master_key, plaintext_key, file_iv_int
    )

    with ThreadPoolExecutor(max_workers=max_worker_count) as executor_input:
        future_results_input = []

        for file_number in range(1, int(file_count) + 1):
            future_results_input.append(
                executor_input.submit(
                    _generate_input_files_with_different_timestamps,
                    s3_bucket,
                    s3_prefix,
                    file_number,
                    record_count,
                    file_count,
                    short_topic,
                    file_iv_whole,
                    input_base_content,
                    encrypted_key,
                    plaintext_key,
                    master_key,
                    input_folder,
                    encryption_json_text,
                )
            )

        wait(future_results_input)
        for future in future_results_input:
            try:
                yield future.result()
            except Exception as ex:
                console_printer.print_error_text(
                    f"Individual file generation failed with error: '{ex}'"
                )


def _generate_input_files_with_different_timestamps(
    s3_bucket,
    s3_prefix,
    file_number,
    record_count,
    file_count,
    short_topic,
    file_iv_whole,
    input_base_content,
    encrypted_key,
    plaintext_key,
    master_key,
    input_folder,
    encryption_json_text,
):
    """Generates required historic data files from the files in the given folder.

    Keyword arguments:
    s3_bucket -- the s3 bucket to send input files to
    s3_prefix -- the s3 prefix to send input files to
    file_number -- the number of the current file
    record_count -- the number of records per file
    file_count -- the number of files
    short_topic -- the short topic name
    file_iv_whole -- the whole of the IV for the metadata file
    input_base_content -- the content of the input template file
    encrypted_key -- the encrypted version of the plaintext key
    plaintext_key -- the plaintext data key for encrypting the data file
    master_key -- the master key used to encrypt the data key
    input_folder -- the folder to store the generated input files in
    encryption_json_text -- the test for the metadata file
    """
    global key_method

    console_printer.print_info(
        f"Writing file {str(file_number)} with {str(record_count)} records in each using key method {key_method} for topic {short_topic}"
    )

    padded_number = str(file_number).zfill(4)
    input_file = f"{short_topic}.{padded_number}.{import_data_file_extension}"
    metadata_file_name = (
        f"{short_topic}.{padded_number}.{import_metadata_file_extension}"
    )
    metadata_file = os.path.join(input_folder, metadata_file_name)

    generate_encryption_input_metadata_file(metadata_file, encryption_json_text)

    generate_input_file(
        file_number,
        input_base_content,
        input_folder,
        input_file,
        record_count,
        file_count,
        encrypted_key,
        plaintext_key,
        master_key,
        file_iv_whole,
    )

    console_printer.print_info(
        f"Uploading metadata file with name of {metadata_file_name} to "
        + f"{s3_prefix} in bucket {s3_bucket}"
    )

    aws_helper.upload_file_to_s3_and_wait_for_consistency(
        metadata_file,
        s3_bucket,
        context.timeout,
        os.path.join(s3_prefix, metadata_file_name),
    )

    console_printer.print_info(
        f"Uploading data file with name of {input_file} to "
        + f"{s3_prefix} in bucket {s3_bucket}"
    )

    aws_helper.upload_file_to_s3_and_wait_for_consistency(
        os.path.join(input_folder, input_file),
        s3_bucket,
        context.timeout,
        os.path.join(s3_prefix, input_file),
    )

    return input_file


def generate_input_file(
    file_number,
    input_base_content,
    input_folder,
    input_file,
    record_count,
    file_count,
    encrypted_key,
    plaintext_key,
    master_key,
    initialisation_vector,
):
    """Creates a local input file for historic data and returns the location and file name.

    Keyword arguments:
    file_number -- the number of the current file per topic
    input_base_content -- the content of the input template file
    input_folder -- the location to create input files in
    input_file -- the filename to create
    record_count -- the number of records per file
    file_count -- the number of desired files
    encrypted_key -- the encrypted version of the plaintext key
    plaintext_key -- the plaintext data key for encrypting the data file
    master_key -- the master key used to encrypt the data key
    initialisation_vector -- the initialisation vector to use for the encryption
    """
    global keys
    global key_method

    console_printer.print_info(f"Generating input file number {str(file_number)}")

    local_keys = keys
    file_contents = ""
    file_full_path = os.path.join(input_folder, input_file)

    for record_number in range(1, int(record_count) + 1):
        current_key_index = get_current_key_index(
            key_method, file_number, record_number, record_count
        )
        key_for_record = local_keys[current_key_index][0]

        (timestamp, timestamp_string) = date_helper.add_milliseconds_to_timestamp(
            _base_datetime_timestamp, file_number + record_number, True
        )

        db_object = generate_uncrypted_record(
            timestamp_string, input_base_content, key_for_record, plaintext_key
        )

        file_contents += json.dumps(json.loads(db_object)) + "\n"

    encrypted_contents = generate_encrypted_record(
        initialisation_vector, file_contents, plaintext_key, True
    )

    with open(file_full_path, "wb") as data:
        data.write(encrypted_contents)


def get_current_key_index(
    key_method, current_file_number, current_record_number, record_count
):
    """Returns the current key with details.

    Keyword arguments:
    key_method -- the key method to use
    current_file_number -- the number of the current file being processed
    current_record_number -- the number of the current record being processed
    record_count -- the number of desired records per file
    """
    current_key_index = 0

    if key_method == "different":
        files_already_done = current_file_number - 1
        current_record = (files_already_done * record_count) + current_record_number
        current_key_index = current_record - 1
    elif key_method == "file":
        current_key_index = current_file_number - 1
    elif key_method == "record":
        current_key_index = current_record_number - 1

    return current_key_index


def generate_encryption_input_metadata_file(metadata_file, encryption_json_text):
    """Encrypts the supplied bytes with the supplied key.
    Returns the initialisation vector and the encrypted data as a tuple.

    Keyword arguments:
    metadata_file -- the full file name and location to create
    encryption_json_text -- the text for the file
    """
    with open(metadata_file, "w") as metadata:
        metadata.write(encryption_json_text)


def generate_encryption_metadata_for_metadata_file(
    encrypted_key, master_key, plaintext_key, initialisation_vector
):
    """Encrypts the supplied bytes with the supplied key.
    Returns the initialisation vector and the encrypted data as a tuple.

    Keyword arguments:
    encrypted_key -- the encrypted version of the plaintext key
    master_key -- the master key used to encrypt the data key
    plaintext_key -- the plaintext data key for encrypting the data file
    initialisation_vector -- the initialisation vector to use for the encryption
    """
    encryption_metadata = {
        "encryptedEncryptionKey": encrypted_key,
        "keyEncryptionKeyId": master_key,
        "plaintextDatakey": plaintext_key,
        "initialisationVector": base64.b64encode(initialisation_vector).decode("ascii"),
        "keyEncryptionKeyHash": "TEST",
    }

    return json.dumps(encryption_metadata, indent=4)


def generate_encrypted_record(
    initialisation_vector, unencrypted_data, plaintext_key, compress
):
    """Generates formatted individual record and returns the iv and the record as a tuple.

    Keyword arguments:
    initialisation_vector -- the initialisation vector to use for the encryption
    unencrypted_data -- the input template text
    plaintext_key -- the plaintext data key for encrypting the data file
    compress -- True or False to compres before encryption
    """
    compressed = (
        gzip.compress(unencrypted_data.encode("ascii"))
        if compress
        else unencrypted_data.encode("utf8")
    )
    return encrypt(initialisation_vector, plaintext_key, compressed)


def generate_uncrypted_record(timestamp, input_template_text, key, plaintext_key):
    """Generates formatted individual record and returns the iv and the record as a tuple.

    Keyword arguments:
    timestamp -- the timestamp for the record
    input_template_text -- the input template text
    key -- the key for the id
    plaintext_key -- the plaintext data key for encrypting the data file
    """
    return input_template_text.replace("||newid||", str(key)).replace(
        "||timestamp||", timestamp
    )


def generate_initialisation_vector():
    """Generates an initialisation vector for encryption."""
    initialisation_vector = Random.new().read(AES.block_size)
    return (initialisation_vector, int(binascii.hexlify(initialisation_vector), 16))


def encrypt(initialisation_vector, datakey, unencrypted_bytes):
    """Encrypts the supplied bytes with the supplied key.
    Returns the initialisation vector and the encrypted data as a tuple.

    Keyword arguments:
    initialisation_vector -- the initialisation vector to use for the encryption
    datakey -- the key to use for the encryption
    unencrypted_bytes -- the unencrypted data to encrypt
    """
    counter = Counter.new(AES.block_size * 8, initial_value=initialisation_vector)
    aes = AES.new(base64.b64decode(datakey), AES.MODE_CTR, counter=counter)

    return aes.encrypt(unencrypted_bytes)
