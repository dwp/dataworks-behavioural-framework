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
from helpers import file_helper, template_helper, date_helper, console_printer
from helpers import snapshot_data_generator


import_data_file_extension = "json.gz.enc"
import_metadata_file_extension = "json.encryption.json"

keys = [[uuid.uuid4(), "", ""]]
key_method = "single"
_base_datetime_timestamp = datetime.strptime(
    "2018-11-01T03:02:01.001", "%Y-%m-%dT%H:%M:%S.%f"
)
_base_datetime_created = datetime.strptime(
    "2018-12-02T04:03:02.002", "%Y-%m-%dT%H:%M:%S.%f"
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


def generate_historic_data(
    test_run_name,
    method,
    file_count,
    record_count,
    topic,
    input_template,
    output_template,
    fixture_files_root,
    snapshot_record_template_name,
    encrypted_key,
    plaintext_key,
    master_key,
    input_folder,
    output_folder,
    dbobject_iv_tuple,
    file_iv_tuple,
    snapshots_output_folder,
    static_key,
    unique_number,
):
    """Generates required historic data files from the files in the given folder.

    Keyword arguments:
    test_run_name -- the unique name for this test run
    method -- 'single', 'different', 'file' or 'record' for the key method to use for this data
    file_count -- the number of files to create for the collection
    record_count -- the number of records per file
    topic -- the topic (contains db.collection)
    input_template -- the name and location for the input template json file
    output_template -- the name and location for the output template json file
    fixture_files_root -- the root folder for the template folders
    snapshot_record_template_name -- the snapshot record template file
    encrypted_key -- the encrypted version of the plaintext key
    plaintext_key -- the plaintext data key for encrypting the data file
    master_key -- the master key used to encrypt the data key
    input_folder -- the folder to store the generated input files in
    output_folder -- the folder to store the generated output files in (if None, no output will be generated)
    dbobject_iv_tuple -- a tuple for the initialisation vector to use for the dbobject (random number, full vector)
    file_iv_tuple -- a tuple for the initialisation vector to use for the whole file (random number, full vector)
    snapshots_output_folder -- the folder name for the unencypted files (if None, no unencrypted files will be generated)
    static_key -- the key to use for the messages or None
    unique_number -- a unique number for this specific test, must change every time method is called
    """
    global key_method
    key_method = method

    _generate_keys(file_count, record_count, static_key)

    console_printer.print_info(
        f"Generating {str(file_count)} files with {str(record_count)} records in each using key method {key_method} for topic {topic}"
    )

    short_topic = template_helper.get_short_topic_name(topic)

    encryption_json_text_output = generate_encryption_metadata_for_dbobject(
        encrypted_key, master_key, plaintext_key, file_iv_tuple[0]
    )

    job_id = str(uuid.uuid4())

    if os.path.exists(input_folder):
        file_helper.clear_and_delete_directory(input_folder)
    os.makedirs(input_folder)

    for result_input in _generate_input_data_files_threaded(
        file_count,
        record_count,
        short_topic,
        input_template,
        encrypted_key,
        plaintext_key,
        master_key,
        input_folder,
        dbobject_iv_tuple,
        unique_number,
    ):
        console_printer.print_info(f"Generated input file {result_input}")

    if output_folder is not None:
        if os.path.exists(output_folder):
            file_helper.clear_and_delete_directory(output_folder)
        os.makedirs(output_folder)

        for result_output in _generate_output_data_files_threaded(
            short_topic,
            output_template,
            plaintext_key,
            output_folder,
            encryption_json_text_output,
            file_iv_tuple[1],
            job_id,
        ):
            console_printer.print_info(f"Generated output file {result_output}")

    if snapshot_record_template_name is not None:
        for result_snapshot in _generate_snapshot_record_data_files_threaded(
            test_run_name,
            topic,
            fixture_files_root,
            snapshot_record_template_name,
            snapshots_output_folder,
        ):
            console_printer.print_info(
                f"Generated snapshot record file {result_snapshot}"
            )


def _generate_input_data_files_threaded(
    file_count,
    record_count,
    short_topic,
    input_template,
    encrypted_key,
    plaintext_key,
    master_key,
    input_folder,
    dbobject_iv_tuple,
    unique_number,
):
    """Generates required historic data files from the files in the given folder using multiple threads.

    Keyword arguments:
    file_count -- the number of files to create for the collection
    record_count -- the number of records per file
    short_topic -- the short topic
    input_template -- the name and location for the input template json file
    encrypted_key -- the encrypted version of the plaintext key
    plaintext_key -- the plaintext data key for encrypting the data file
    master_key -- the master key used to encrypt the data key
    input_folder -- the folder to store the generated input files in
    dbobject_iv_tuple -- a tuple for the initialisation vector to use for the dbobject (random number, full vector)
    unique_number -- a unique number for this specific test, must change every time method is called
    """
    input_base_content = file_helper.get_contents_of_file(input_template, False)

    encryption_json_text = generate_encryption_metadata_for_metadata_file(
        encrypted_key, master_key, plaintext_key, dbobject_iv_tuple[0]
    )

    with ThreadPoolExecutor() as executor_input:
        future_results_input = []

        for file_number in range(1, int(file_count) + 1):
            future_results_input.append(
                executor_input.submit(
                    _generate_input_files_with_different_timestamps,
                    file_number,
                    record_count,
                    file_count,
                    short_topic,
                    dbobject_iv_tuple[1],
                    input_base_content,
                    encrypted_key,
                    plaintext_key,
                    master_key,
                    input_folder,
                    encryption_json_text,
                    unique_number,
                )
            )

        wait(future_results_input)
        for future in future_results_input:
            try:
                yield future.result()
            except Exception as ex:
                raise AssertionError(ex)


def _generate_snapshot_record_data_files_threaded(
    test_run_name,
    topic_name,
    fixture_files_root,
    snapshot_record_template_name,
    snapshots_output_folder,
):
    """Generates required historic data files from the files in the given unencrypted folder using multiple threads.

    Keyword arguments:
    test_run_name -- unique name for this test run
    topic_name -- the topic name which records are generated for
    fixture_files_root -- the local path to the feature file to send
    snapshot_record_template_name -- the snapshot record template file
    snapshots_output_folder -- the folder to store the files in
    """

    global keys
    local_keys = keys

    with ThreadPoolExecutor() as executor_output:
        future_results_snapshot_record = []
        for key_number in range(0, len(local_keys)):
            future_results_snapshot_record.append(
                executor_output.submit(
                    generate_snapshot_record_file,
                    test_run_name,
                    topic_name,
                    fixture_files_root,
                    snapshot_record_template_name,
                    snapshots_output_folder,
                    local_keys[key_number],
                )
            )

        wait(future_results_snapshot_record)
        for future in future_results_snapshot_record:
            try:
                yield future.result()
            except Exception as ex:
                raise AssertionError(ex)


def generate_snapshot_record_file(
    test_run_name,
    topic_name,
    fixture_files_root,
    snapshot_record_template_name,
    snapshots_output_folder,
    key_data,
):
    """Creates a local input file for historic data and returns the location and file name.

    Keyword arguments:
    test_run_name -- unique name for this test run
    topic_name -- the topic name which records are generated for
    fixture_files_root -- the local path to the feature file to send
    snapshot_record_template_name -- the snapshot record template file
    snapshots_output_folder -- the folder to store the files in
    key_data -- the key data from the global list
    """
    return snapshot_data_generator.generate_hbase_record_for_snapshot_file(
        snapshot_record_template_name,
        key_data[2],
        key_data[0],
        "E2E_IMPORT",
        test_run_name,
        topic_name,
        fixture_files_root,
        snapshots_output_folder,
    )


def _generate_output_data_files_threaded(
    short_topic,
    output_template,
    plaintext_key,
    output_folder,
    encryption_json_text_output,
    output_iv_full,
    job_id,
):
    """Generates required historic data files from the files in the given folder using multiple threads.

    Keyword arguments:
    short_topic -- the short topic
    output_template -- the name and location for the output template json file
    plaintext_key -- the plaintext data key for encrypting the data file
    output_folder -- the folder to store the generated output files in
    encryption_json_text_output -- the encryption text
    output_iv_full -- the iv used to encrypt
    job_id -- job id for the messages
    """
    global keys
    local_keys = keys

    output_base_content = file_helper.get_contents_of_file(output_template, False)

    with ThreadPoolExecutor() as executor_output:
        future_results_output = []
        for key_number in range(0, len(local_keys)):
            future_results_output.append(
                executor_output.submit(
                    generate_output_file,
                    output_base_content,
                    output_folder,
                    local_keys[key_number],
                    encryption_json_text_output,
                    short_topic,
                    plaintext_key,
                    output_iv_full,
                    job_id,
                    key_number + 1,
                )
            )

        wait(future_results_output)
        for future in future_results_output:
            try:
                yield future.result()
            except Exception as ex:
                raise AssertionError(ex)


def _generate_input_files_with_different_timestamps(
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
    unique_number,
):
    """Generates required historic data files from the files in the given folder.

    Keyword arguments:
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
    unique_number -- a unique number for this specific test, must change every time method is called
    """
    global key_method

    console_printer.print_info(
        f"Writing file {str(file_number)} with {str(record_count)} records in each using key method {key_method} for topic {short_topic}"
    )

    padded_number = str(file_number).zfill(4)
    unique_number_string = str(unique_number)

    input_file = f"{short_topic}.{unique_number_string}{padded_number}.{import_data_file_extension}"
    metadata_file_name = f"{short_topic}.{unique_number_string}{padded_number}.{import_metadata_file_extension}"
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

    return input_file


def generate_output_file(
    output_base_content,
    output_folder,
    key_data,
    encryption_field,
    short_topic,
    plaintext_key,
    initialisation_vector,
    job_id,
    key_count,
):
    """Creates a local input file for historic data and returns the location and file name.

    Keyword arguments:
    output_base_content -- the content of the template output file
    output_folder -- the folder to store the output files in
    key -- the key data from the global list
    encryption_field -- the value for the encryption field
    short_topic -- the topic (does not contain 'db.')
    plaintext_key -- the plaintext data key for encrypting the data file
    initialisation_vector -- the initialisation vector to use for the encryption
    job_id -- the guid that identifies this run of the import
    key_count -- the count of the current key
    """
    encrypted_record = generate_encrypted_record(
        initialisation_vector, key_data[1], plaintext_key, False
    )

    output_file = os.path.join(output_folder, f"{str(key_data[0])}.json")

    (timestamp, timestamp_string) = date_helper.add_milliseconds_to_timestamp(
        _base_datetime_created, key_count, False
    )

    db_and_collection = short_topic.split(".")

    formatted_output = (
        output_base_content.replace("||jobId||", job_id)
        .replace("||timeOfMessageCreation||", timestamp_string)
        .replace("||traceId||", str(uuid.uuid4()))
        .replace("||collection||", db_and_collection[1])
        .replace("||db||", db_and_collection[0])
        .replace("||newid||", str(key_data[0]))
        .replace("||timestamp||", key_data[2])
        .replace("||encryption||", encryption_field)
        .replace("||dbObject||", base64.b64encode(encrypted_record).decode("ascii"))
    )

    with open(output_file, "wt") as data:
        data.write(formatted_output)

    return output_file


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
            timestamp_string, input_base_content, key_for_record
        )

        file_contents += json.dumps(json.loads(db_object)) + "\n"
        update_key_data(
            current_key_index,
            db_object,
            timestamp_string,
            file_number,
            record_number,
            record_count,
            file_count,
        )

    encrypted_contents = generate_encrypted_record(
        initialisation_vector, file_contents, plaintext_key, True
    )

    with open(file_full_path, "wb") as data:
        data.write(encrypted_contents)


def update_key_data(
    key_index,
    db_object,
    timestamp,
    current_file_number,
    current_record_number,
    record_count,
    file_count,
):
    """Updates the current key data with details if required.

    Keyword arguments:
    key_index -- the current key index
    db_object -- the db object to update with
    timestamp -- the timestamp to update with
    current_file_number -- the number of the current file being processed
    current_record_number -- the number of the current record being processed
    record_count -- the number of desired records per file
    file_count -- the number of desired files
    """
    global key_method
    global keys

    should_update = False

    if key_method == "different":  # Update always as each key is a different record
        should_update = True
    elif key_method == "single":  # Only update the key when on the last record
        if current_record_number == record_count and current_file_number == file_count:
            should_update = True
    elif key_method == "file":  # Only update key when on the last record for each file
        if current_record_number == record_count:
            should_update = True
    elif key_method == "record":  # Only update when on the last file for each record
        if current_file_number == file_count:
            should_update = True

    if should_update:
        with _keys_lock:
            keys[key_index][1] = db_object
            keys[key_index][2] = timestamp


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


def generate_encryption_metadata_for_dbobject(
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
    }

    return json.dumps(encryption_metadata, indent=4)


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


def generate_uncrypted_record(timestamp, input_template_text, key):
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
    iv_tuple = (initialisation_vector, int(binascii.hexlify(initialisation_vector), 16))

    console_printer.print_info(f"Generated IV tuple of '{iv_tuple}'")

    return iv_tuple


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
