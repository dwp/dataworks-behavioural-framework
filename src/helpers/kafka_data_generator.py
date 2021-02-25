import os
import json
import fnmatch
import time
import shutil
import uuid
import threading
from datetime import datetime
from helpers import (
    aws_helper,
    template_helper,
    date_helper,
    console_printer,
    file_helper,
)
from helpers import snapshot_data_generator


_base_datetime_timestamp = datetime.strptime(
    "2018-11-01T03:02:01.001", "%Y-%m-%dT%H:%M:%S.%f"
)


def generate_kafka_files(
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
    snapshots_output_folder,
    seconds_timeout,
    fixture_data_folder,
    dlq_template_name=None,
    snapshot_record_template_name=None,
    with_timestamp=True,
    custom_base_timestamp=None,
):
    """Returns array of generated kafka data as tuples of (input s3 location, output local file).

    Keyword arguments:
    test_run_name -- unique name for this test run
    s3_input_bucket - the bucket for the remote fixture files
    input_template_name -- the input template file
    output_template_name -- the output template file (None if no output needed)
    dlq_template_name -- the output template file (None if no dlq file needed)
    snapshot_record_template_name -- the snapshot record template file (None if no dlq file needed)
    new_uuid -- the uuid to use for the id (None to generate one for each file)
    local_files_temp_folder -- the root folder for the temporary files to sit in
    fixture_files_root -- the local path to the feature file to send
    s3_output_prefix -- the output path for the edited file in s3
    with_timestamp -- True for adding a timestamp to the files
    custom_base_timestamp -- if adding a timestamp, override here or generic base will be used
    record_count -- the number of records to send
    topic_name -- the topic name which records are generated for
    snapshots_output_folder -- the snapshots output folder (None if no snapshot file needed)
    seconds_timeout -- the timeout in seconds for the test
    fixture_data_folder -- the folder from the root of the fixture data
    """
    base_timestamp = (
        _base_datetime_timestamp
        if custom_base_timestamp is None
        else custom_base_timestamp
    )
    file_string = "files" if int(record_count) > 1 else "file"

    console_printer.print_info(
        f"Generating '{record_count}' Kafka '{file_string}' for topic of '{topic_name}' "
        + f"using input template of '{input_template_name}', output template of '{output_template_name}', "
        + f"id of '{new_uuid}' and base timestamp of '{base_timestamp}'"
    )

    generated_files = []

    for record_number in range(1, int(record_count) + 1):
        key = new_uuid if new_uuid is not None else uuid.uuid4()
        (timestamp, timestamp_string) = (
            (None, None)
            if not with_timestamp
            else date_helper.add_milliseconds_to_timestamp(
                base_timestamp, record_number, True
            )
        )

        generated_files.append(
            _generate_kafka_file(
                test_run_name,
                s3_input_bucket,
                input_template_name,
                output_template_name,
                dlq_template_name,
                snapshot_record_template_name,
                key,
                local_files_temp_folder,
                fixture_files_root,
                s3_output_prefix,
                topic_name,
                snapshots_output_folder,
                seconds_timeout,
                fixture_data_folder,
                timestamp_string,
            )
        )

    return generated_files


def _generate_kafka_file(
    test_run_name,
    s3_input_bucket,
    input_template_name,
    output_template_name,
    dlq_template_name,
    snapshot_record_template_name,
    new_uuid,
    local_files_temp_folder,
    fixture_files_root,
    s3_output_prefix,
    topic_name,
    snapshots_output_folder,
    seconds_timeout,
    fixture_data_folder,
    timestamp=None,
):
    """Generates Kafka input and check files, returns tuple of (input s3 location, output local file).

    Keyword arguments:
    test_run_name -- unique name for this test run
    s3_input_bucket - the bucket for the remote fixture files
    input_template_name -- the input template file
    output_template_name -- the output template file (None if no output needed)
    dlq_template_name -- the output template file (None if no dlq file needed)
    snapshot_record_template_name -- the snapshot record template file (None if no snapshot file needed)
    new_uuid -- the uuid to use for the id
    local_files_temp_folder -- the root folder for the temporary files to sit in
    fixture_files_root -- the local path to the feature file to send
    s3_output_prefix -- the output path for the edited file in s3
    topic_name -- the topic name which records are generated for
    snapshots_output_folder -- the snapshots output folder (None if no snapshot file needed)
    seconds_timeout -- the timeout in seconds for the test
    fixture_data_folder -- the folder from the root of the fixture data
    timestamp -- set to a value to override timestamp in file
    """
    timestamp_string = "NOT_SET" if timestamp is None else str(timestamp)
    console_printer.print_debug(
        f"Generating Kafka file for input template of {input_template_name}, output template of {output_template_name}, id of {new_uuid} and timestamp of {timestamp_string}"
    )

    input_s3_prefix, input_file_local = _generate_kafka_input_file(
        s3_input_bucket,
        fixture_data_folder,
        input_template_name,
        new_uuid,
        local_files_temp_folder,
        fixture_files_root,
        s3_output_prefix,
        seconds_timeout,
        timestamp,
    )

    output_local_file = None
    if output_template_name is not None:
        output_local_file = _generate_kafka_output_file(
            s3_input_bucket,
            fixture_data_folder,
            output_template_name,
            new_uuid,
            local_files_temp_folder,
            fixture_files_root,
            timestamp,
        )

    dlq_local_file = None
    if dlq_template_name is not None:
        dlq_local_file = _generate_kafka_dlq_file(
            s3_input_bucket,
            fixture_data_folder,
            dlq_template_name,
            new_uuid,
            local_files_temp_folder,
            fixture_files_root,
            timestamp,
        )

    snapshot_local_file = None
    if snapshot_record_template_name is not None:
        snapshot_local_file = (
            snapshot_data_generator.generate_hbase_record_for_snapshot_file(
                snapshot_record_template_name,
                timestamp,
                new_uuid,
                "E2E_KAFKA",
                test_run_name,
                topic_name,
                fixture_files_root,
                snapshots_output_folder,
            )
        )

    return (
        input_s3_prefix,
        input_file_local,
        output_local_file,
        dlq_local_file,
        snapshot_local_file,
    )


def _generate_kafka_input_file(
    s3_input_bucket,
    local_folder,
    template_file_name,
    new_uuid,
    local_files_temp_folder,
    fixture_files_root,
    s3_output_prefix,
    seconds_timeout,
    timestamp=None,
):
    """Generates a Kafka input file and sends it to S3 and returns remote location.

    Keyword arguments:
    s3_input_bucket - the bucket for the remote fixture files
    local_folder -- the local parent folder for the template file
    template_file_name -- the template file name
    new_uuid -- the uuid to use for the id
    local_files_temp_folder -- the root folder for the temporary files to sit in
    fixture_files_root -- the local path to the feature file to send
    s3_output_prefix -- the output path for the edited file in s3
    seconds_timeout -- the timeout in seconds for the test
    timestamp -- set to a value to override timestamp in file
    """
    timestamp_string = "NOT_SET" if timestamp is None else str(timestamp)
    console_printer.print_debug(
        f"Generating Kafka input file for template of {template_file_name} and id of {new_uuid} and timestamp of {timestamp_string}"
    )

    file_name = os.path.join(fixture_files_root, local_folder, template_file_name)

    with open(file_name) as open_file:
        record = open_file.read()

    record = record.replace("||newid||", f"{new_uuid}")
    if timestamp is not None:
        record = record.replace("||timestamp||", f"{timestamp}")

    output_files_folder = os.path.join(local_folder, "input-only-files")
    output_file_local = file_helper.generate_local_output_file(
        output_files_folder, template_file_name, local_files_temp_folder
    )
    console_printer.print_debug(f"Writing Kafka input file to '{output_file_local}'")
    with open(f"{output_file_local}", "w") as output_file_data:
        output_file_data.write(record)

    output_file_full_path_s3 = os.path.join(s3_output_prefix, str(new_uuid))
    full_file_path_s3 = os.path.join(output_file_full_path_s3, template_file_name)
    aws_helper.upload_file_to_s3_and_wait_for_consistency(
        output_file_local, s3_input_bucket, seconds_timeout, full_file_path_s3
    )

    return (full_file_path_s3, output_file_local)


def _generate_kafka_output_file(
    s3_input_bucket,
    local_folder,
    template_file_name,
    new_uuid,
    local_files_temp_folder,
    fixture_files_root,
    timestamp=None,
):
    """Generates a kafka output value to check against HBase and returns lcoal file name for edited file.

    Keyword arguments:
    s3_input_bucket - the bucket for the remote fixture files
    local_folder -- the local parent folder for the template file
    template_file_name -- the template file name
    new_uuid -- the uuid to use for the id
    local_files_temp_folder -- the root folder for the temporary files to sit in
    fixture_files_root -- the local path to the feature file to send
    timestamp -- set to a value to override timestamp in file
    """
    timestamp_string = "NOT_SET" if timestamp is None else str(timestamp)
    console_printer.print_debug(
        f"Generating Kafka output file for template of {template_file_name} and id of {new_uuid} and timestamp of {timestamp_string}"
    )

    file_name = os.path.join(fixture_files_root, local_folder, template_file_name)

    with open(file_name) as open_file:
        record = open_file.read()

    record = record.replace("||newid||", f"{new_uuid}")
    if timestamp is not None:
        record = record.replace("||timestamp||", f"{timestamp}")

    output_file_local = file_helper.generate_local_output_file(
        local_folder, template_file_name, local_files_temp_folder
    )
    console_printer.print_debug(f"Writing Kafka output file to '{output_file_local}'")
    with open(f"{output_file_local}", "w") as output_file_data:
        output_file_data.write(record)

    return output_file_local


def _generate_kafka_dlq_file(
    s3_input_bucket,
    local_folder,
    template_file_name,
    new_uuid,
    local_files_temp_folder,
    fixture_files_root,
    timestamp=None,
):
    """Generates a kafka dlq file to check against HBase and returns local file name for edited file.

    Keyword arguments:
    s3_input_bucket - the bucket for the remote fixture files
    local_folder -- the local parent folder for the template file
    template_file_name -- the template file name
    new_uuid -- the uuid to use for the id
    local_files_temp_folder -- the root folder for the temporary files to sit in
    fixture_files_root -- the local path to the feature file to send
    timestamp -- set to a value to override timestamp in file
    """
    timestamp_string = "NOT_SET" if timestamp is None else str(timestamp)
    console_printer.print_debug(
        f"Generating Kafka dlq file for template of {template_file_name} and id of {new_uuid} and timestamp of {timestamp_string}"
    )

    file_name = os.path.join(fixture_files_root, local_folder, template_file_name)

    with open(file_name) as open_file:
        record = open_file.read()

    record = record.replace("||newid||", f"{new_uuid}")

    output_file_local = file_helper.generate_local_output_file(
        local_folder, template_file_name, local_files_temp_folder
    )
    console_printer.print_debug(f"Writing Kafka dlq file to '{output_file_local}'")
    with open(f"{output_file_local}", "w") as output_file_data:
        output_file_data.write(record)

    return output_file_local
