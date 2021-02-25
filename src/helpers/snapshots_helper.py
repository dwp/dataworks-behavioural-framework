import os
import time
import json
from helpers import template_helper, aws_helper, file_helper


def generate_snapshot_output_s3_path(
    base_prefix, topic_name, db_name, formatted_date, snapshot_type
):
    """Generate the snapshot output s3 path

    Keyword arguments:
    base_prefix -- unique name for this test run
    topic_name -- singular topic name
    db_name -- name of the database
    formatted_date -- the current formatted date
    snapshot_type -- the type of snapshots, either full or incremental
    """
    (
        database,
        collection,
    ) = template_helper.get_database_and_collection_from_topic_name(topic_name)
    return os.path.join(base_prefix, snapshot_type, formatted_date, db_name, collection)


def wait_for_snapshots_to_be_sent_to_s3(
    timeout,
    number_of_snapshots_expected_minimum,
    snapshot_output_bucket,
    snapshot_output_prefix,
):
    """Waits for at least one snapshot to be pushed to s3 and returns a boolean for success.

    Keyword arguments:
    timeout -- timeout in seconds
    number_of_snapshots_expected_minimum -- min number of snapshots needed
    snapshot_output_bucket -- the bucket snapshots are sent to
    snapshot_output_prefix -- the path snapshots are sent to
    """
    number_of_snapshots_actual = 0
    count = 1
    while (
        number_of_snapshots_actual < number_of_snapshots_expected_minimum
        and count < timeout
    ):
        number_of_snapshots_actual = aws_helper.get_number_of_s3_objects_for_key(
            snapshot_output_bucket, snapshot_output_prefix
        )
        time.sleep(1)
        count += 1

    return number_of_snapshots_actual >= number_of_snapshots_expected_minimum


def retrieve_records_from_snapshots(snapshot_output_bucket, snapshot_output_prefix):
    """Gets all records from within the snapshots found and returns them in an array.

    Keyword arguments:
    number_of_snapshots_expected_minimum -- min number of snapshots needed
    snapshot_output_bucket -- the bucket snapshots are sent to
    snapshot_output_prefix -- the path snapshots are sent to
    """
    snapshot_files = aws_helper.retrieve_files_from_s3_with_bucket_and_path(
        None, snapshot_output_bucket, snapshot_output_prefix
    )

    snapshot_records = []
    for snapshot_file in snapshot_files:
        snapshot_file_s3_name = snapshot_file["Key"]
        snapshot_file_s3_contents = aws_helper.get_s3_object(
            None, snapshot_output_bucket, snapshot_file_s3_name
        )
        for line in snapshot_file_s3_contents.splitlines():
            unsorted_json_record = json.loads(line)
            snapshot_records.append(json.dumps(unsorted_json_record, sort_keys=True))

    return snapshot_records


def get_locally_generated_snapshot_file_records(snapshot_file):
    """Returns the sorted contents of the local snapshot file as an array.

    Keyword arguments:
    snapshot_file -- full path to the file
    """
    expected_snapshot_file_s3_contents = file_helper.get_contents_of_file(
        snapshot_file, False
    )

    snapshot_records = []
    for line in expected_snapshot_file_s3_contents.splitlines():
        unsorted_json_record = json.loads(line)
        snapshot_records.append(json.dumps(unsorted_json_record, sort_keys=True))

    return snapshot_records


def get_snapshot_run_correlation_id(test_run_name, snapshot_type):
    """Returns the qualified string to use as correlation id.

    Keyword arguments:
    test_run_name -- the test run name for this run
    snapshot_type -- either 'full' or 'incremental'
    """
    return f"{test_run_name}_{snapshot_type}"
