import os
import json
import fnmatch
import time
import shutil
import uuid
import threading
import gzip
from datetime import datetime, timedelta
from helpers import (
    aws_helper,
    template_helper,
    date_helper,
    console_printer,
    file_helper,
    data_load_helper,
)


_base_datetime_timestamp = datetime.strptime(
    "2018-11-01T03:02:01.001", "%Y-%m-%dT%H:%M:%S.%f"
)


def generate_corporate_data_files(
    test_run_name,
    s3_input_bucket,
    input_template_name,
    output_template_name,
    new_uuid,
    local_files_temp_folder,
    fixture_files_root,
    s3_output_prefix,
    record_count,
    topic_name,
    seconds_timeout,
    timestamp_override,
    s3_output_prefix_override=None,
):
    """Returns array of generated corporate data as tuples of (input s3 location, input local file, output local file).

    Keyword arguments:
    test_run_name -- unique name for this test run
    s3_input_bucket - the bucket for the remote fixture files
    input_template_name -- the input template file
    output_template_name -- the output template file
    new_uuid -- the uuid to use for the id (None to generate one for each file)
    local_files_temp_folder -- the root folder for the temporary files to sit in
    fixture_files_root -- the local path to the feature file to send
    s3_output_prefix -- the output path for the edited file in s3
    record_count -- the number of records to send
    topic_name -- the topic name which records are generated for
    timestamp_override -- the base timestamp or None to use default of "2018-11-01T03:02:01.001
    s3_output_prefix_override -- completely override s3_output_prefix, default s3_output_prefix construction will be ignored
        if specified, `s3_output_prefix` needs to be set to an empty string
    """
    global _base_datetime_timestamp
    timestamp_to_use = (
        timestamp_override
        if timestamp_override is not None
        else _base_datetime_timestamp
    )

    file_string = "files" if int(record_count) > 1 else "file"

    console_printer.print_info(
        f"Generating '{record_count}' corporate data '{file_string}' for topic of '{topic_name}' "
        + f"using input template of '{input_template_name}', output template of '{output_template_name}', "
        + f"id of '{new_uuid}' and base timestamp of '{timestamp_to_use}'"
    )

    generated_files = []

    for record_number in range(1, int(record_count) + 1):
        key = new_uuid if new_uuid is not None else uuid.uuid4()
        (timestamp, timestamp_string) = date_helper.add_milliseconds_to_timestamp(
            timestamp_to_use, record_number, True
        )

        generated_files.append(
            _generate_corporate_data_file(
                test_run_name,
                s3_input_bucket,
                input_template_name,
                output_template_name,
                key,
                local_files_temp_folder,
                fixture_files_root,
                s3_output_prefix,
                topic_name,
                seconds_timeout,
                timestamp,
                timestamp_string,
                s3_output_prefix_override,
            )
        )

    return generated_files


def _generate_corporate_data_file(
    test_run_name,
    s3_input_bucket,
    input_template_name,
    output_template_name,
    new_uuid,
    local_files_temp_folder,
    fixture_files_root,
    s3_output_prefix,
    topic_name,
    seconds_timeout,
    timestamp,
    timestamp_string,
    s3_output_prefix_override=None,
):
    """Generates corporate data input and check files, returns tuple of (input s3 location, input local file, output local file).

    Keyword arguments:
    test_run_name -- unique name for this test run
    s3_input_bucket - the bucket for the remote fixture files
    input_template_name -- the input template file
    output_template_name -- the output template file
    new_uuid -- the uuid to use for the id
    local_files_temp_folder -- the root folder for the temporary files to sit in
    fixture_files_root -- the local path to the feature file to send
    s3_output_prefix -- the output path for the edited file in s3
    topic_name -- the topic name which records are generated for
    seconds_timeout -- the timeout in seconds for the test
    timestamp -- the timestamp used in the file
    timestamp_string -- the formatted string of the timestamp
    s3_output_prefix_override -- completely override s3_output_prefix, default s3_output_prefix construction will be ignored
        if specified, `s3_output_prefix` needs to be set to an empty string
    """
    console_printer.print_debug(
        f"Generating corporate datafile for input template of {input_template_name}, output template of {output_template_name}, id of {new_uuid} and timestamp of {timestamp}"
    )

    input_file_local = _generate_corporate_data_input_file(
        "corporate_data",
        input_template_name,
        new_uuid,
        local_files_temp_folder,
        fixture_files_root,
        timestamp_string,
    )

    (output_s3_prefix, output_local_file) = _generate_corporate_data_output_file(
        topic_name,
        s3_input_bucket,
        "corporate_data",
        output_template_name,
        new_uuid,
        local_files_temp_folder,
        fixture_files_root,
        s3_output_prefix,
        seconds_timeout,
        timestamp,
        timestamp_string,
        s3_output_prefix_override,
    )

    return (
        input_file_local,
        output_local_file,
        output_s3_prefix,
    )


def _generate_corporate_data_input_file(
    local_folder,
    template_file_name,
    new_uuid,
    local_files_temp_folder,
    fixture_files_root,
    timestamp_string,
):
    """Generates a corporate datainput file and sends it to S3 and returns local file location.

    Keyword arguments:
    local_folder -- the local parent folder for the template file
    template_file_name -- the template file name
    new_uuid -- the uuid to use for the id
    local_files_temp_folder -- the root folder for the temporary files to sit in
    fixture_files_root -- the local path to the feature file to send
    timestamp_string -- the formatted string of the timestamp in the file
    """
    console_printer.print_debug(
        f"Generating corporate data input file for template of {template_file_name} and id of {new_uuid} and timestamp of {timestamp_string}"
    )

    file_name = os.path.join(fixture_files_root, local_folder, template_file_name)

    with open(file_name) as open_file:
        record = open_file.read()

    record = record.replace("||newid||", f"{new_uuid}")
    record = record.replace("||timestamp||", f"{timestamp_string}")

    output_files_folder = os.path.join(local_folder, "input-only-files")
    output_file_local = file_helper.generate_local_output_file(
        output_files_folder, template_file_name, local_files_temp_folder
    )
    console_printer.print_debug(
        f"Writing corporate data input file to '{output_file_local}'"
    )
    with open(f"{output_file_local}", "w") as output_file_data:
        output_file_data.write(record)

    return output_file_local


def _generate_corporate_data_output_file(
    topic_name,
    s3_input_bucket,
    local_folder,
    template_file_name,
    new_uuid,
    local_files_temp_folder,
    fixture_files_root,
    s3_output_prefix,
    seconds_timeout,
    timestamp,
    timestamp_string,
    s3_output_prefix_override=None,
):
    """Generates a corporate data output value to check against HBase and returns tuple of local and s3 file name for edited file.

    Keyword arguments:
    topic_name -- the topic name
    s3_input_bucket -- the bucket for the remote fixture files
    local_folder -- the local parent folder for the template file
    template_file_name -- the template file name
    new_uuid -- the uuid to use for the id
    local_files_temp_folder -- the root folder for the temporary files to sit in
    fixture_files_root -- the local path to the feature file to send
    s3_output_prefix -- the output path for the edited file in s3
    seconds_timeout -- the timeout in seconds for the test
    timestamp -- the timestamp for the file
    timestamp_string -- the formatted string of the timestamp in the file
    s3_output_prefix_override -- completely override s3_output_prefix, default s3_output_prefix construction will be ignored
        if specified, `s3_output_prefix` needs to be set to an empty string
    """
    console_printer.print_debug(
        f"Generating corporate data output file for template of {template_file_name} and id of {new_uuid} and timestamp of {timestamp_string}"
    )

    file_name = os.path.join(fixture_files_root, local_folder, template_file_name)

    with open(file_name) as open_file:
        record = open_file.read()

    record = record.replace("||newid||", f"{new_uuid}")
    record = record.replace("||timestamp||", f"{timestamp_string}")

    output_file_local = file_helper.generate_local_output_file(
        local_folder, template_file_name, local_files_temp_folder
    )
    console_printer.print_debug(
        f"Writing corporate data output file to '{output_file_local}'"
    )

    with open(f"{output_file_local}", "w") as output_file_data:
        output_file_data.write(record)

    full_file_path_s3 = _upload_data_output_file_to_s3(
        topic_name,
        record,
        s3_input_bucket,
        new_uuid,
        local_files_temp_folder,
        s3_output_prefix,
        seconds_timeout,
        timestamp,
        s3_output_prefix_override,
    )

    return (full_file_path_s3, output_file_local)


def _upload_data_output_file_to_s3(
    topic_name,
    record,
    s3_input_bucket,
    new_uuid,
    local_files_temp_folder,
    s3_output_prefix,
    seconds_timeout,
    timestamp,
    s3_output_prefix_override=None,
):
    """Uploads a gzipped version of the file to S3 and returns full S3 path.

    Keyword arguments:
    topic_name -- the topic name
    record -- the data for the output file
    s3_input_bucket -- the bucket for the remote fixture files
    new_uuid -- the uuid to use for the id
    local_files_temp_folder -- the root folder for the temporary files to sit in
    s3_output_prefix -- the output path for the edited file in s3
    seconds_timeout -- the timeout in seconds for the test
    timestamp -- the timestamp for this file
    s3_output_prefix_override -- completely override s3_output_prefix, default s3_output_prefix construction will be ignored
        if specified, `s3_output_prefix` needs to be set to an empty string
    """
    output_file_local = file_helper.generate_local_output_file(
        "tmp_corporate_data", f"{str(new_uuid)}.gzip", local_files_temp_folder
    )

    console_printer.print_debug(
        f"Writing corporate data tmp gzip file to '{output_file_local}'"
    )

    dumped_record = json.dumps(json.loads(record))

    with gzip.open(output_file_local, "wb") as output_file_data:
        output_file_data.write(dumped_record.encode("utf_8"))

    (
        database,
        collection,
    ) = template_helper.get_database_and_collection_from_topic_name(topic_name)

    file_name = f"{topic_name}_0_1-{str(new_uuid)}.jsonl.gz"
    output_file_full_path_s3 = data_load_helper.generate_corporate_data_s3_prefix(
        s3_output_prefix,
        database,
        collection,
        timestamp,
    )

    if s3_output_prefix_override is not None:
        output_file_full_path_s3 = s3_output_prefix_override

    full_file_path_s3 = os.path.join(output_file_full_path_s3, file_name)
    aws_helper.upload_file_to_s3_and_wait_for_consistency(
        output_file_local, s3_input_bucket, seconds_timeout, full_file_path_s3
    )

    os.remove(output_file_local)

    return full_file_path_s3
