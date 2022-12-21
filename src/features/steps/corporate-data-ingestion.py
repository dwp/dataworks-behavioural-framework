import time
import uuid
import os
import gzip
import json
from behave import given, when, then
from helpers import (
    snapshot_data_generator,
    historic_data_load_generator,
    aws_helper,
    invoke_lambda,
    console_printer,
    emr_step_generator,
    file_helper,
    export_status_helper,
    data_pipeline_metadata_helper,
    corporate_data_generator,
    data_load_helper,
    json_helper,
)
from datetime import datetime

CLUSTER_ARN = "ClusterArn"
COMPLETED_STATUS = "COMPLETED"
CORRELATION_ID = "correlation_id"
S3_PREFIX = "s3_prefix"
EXPORT_DATE = "export_date"
SNAPSHOT_TYPE = "snapshot_type"


@given("the s3 '{type}' prefix is cleared")
def step_impl(context, type):
    if type == "source":
        aws_helper.clear_s3_prefix(
            context.corporate_storage_s3_bucket_id, context.s3_source_prefix, True
        )
    elif type == "destination":
        aws_helper.clear_s3_prefix(
            context.published_bucket, context.s3_destination_prefix, True
        )
    else:
        console_printer.print_error_text(
            f"Executing: clean s3 '{type}' prefix. Unknown type."
        )


@given("the s3 source prefix is set to k2hb landing place in corporate bucket")
def step_impl(context):
    context.s3_source_prefix = f"corporate_storage/ucfs_main/{datetime.now().strftime('%Y/%m/%d')}/automatedtests/{context.test_run_name}_1"


@given("the s3 '{location_type}' prefix replaced by unauthorised location")
def step_impl(context, location_type):
    if location_type == "source":
        context.s3_source_prefix = "unauthorised_location/e2e"
        aws_helper.put_object_in_s3(
            "foobar",
            context.corporate_storage_s3_bucket_id,
            os.path.join(context.s3_source_prefix, "foobar.jsonl.gz"),
        )
    elif location_type == "destination":
        context.s3_destination_prefix = "unauthorised_location/e2e"
    else:
        raise AttributeError(
            "Parameter 'location_type' must be one of two values: 'source' or 'destination'"
        )


@given(
    "we generate a corrupted archive and store it in the Corporate Storage S3 bucket"
)
def step_impl(context):
    filename = f"corrupted_archive.jsonl.gz"
    aws_helper.upload_file_to_s3_and_wait_for_consistency(
        file_location=os.path.join(
            context.fixture_path_local, "corporate_data", filename
        ),
        s3_bucket=context.corporate_storage_s3_bucket_id,
        seconds_timeout=context.timeout,
        s3_key=os.path.join(context.s3_source_prefix, filename),
    )


@when("a step '{step_type}' is triggered on the EMR cluster corporate-data-ingestion")
def step_impl(context, step_type):
    cluster_state = aws_helper.poll_emr_cluster_status(
        cluster_id=context.corporate_data_ingestion_cluster_id,
        timeout_in_seconds=720,
    )

    if cluster_state != "WAITING":
        raise AssertionError("Cluster not in 'WAITING' state before timeout")

    context.step_type = step_type
    context.s3_destination_prefix = os.path.join(
        context.s3_destination_prefix, step_type
    )
    context.correlation_id = f"corporate_data_ingestion_{uuid.uuid4()}"
    context.step_id = emr_step_generator.generate_spark_step(
        emr_cluster_id=context.corporate_data_ingestion_cluster_id,
        script_location="/opt/emr/steps/corporate-data-ingestion.py",
        step_type=f"""automatedtests: {step_type}""",
        command_line_arguments=f"""--correlation_id {context.correlation_id} """
        f"""--source_s3_prefix {context.s3_source_prefix} """
        f"""--destination_s3_prefix {context.s3_destination_prefix} """
        f"""--transition_db_name foo """
        f"""--db_name bar """,
    )


@then("confirm that the EMR step status is '{expected_status}'")
def step_impl(context, expected_status):
    step_status = aws_helper.poll_emr_cluster_step_status(
        context.step_id,
        context.corporate_data_ingestion_cluster_id,
        timeout_in_seconds=600,
    )

    if step_status != expected_status:
        raise AssertionError(
            f"""automatedtests: {context.step_type} step failed with final status of '{step_status}'"""
        )


@then("confirm that '{record_count}' messages have been ingested")
def step_impl(context, record_count):
    result = aws_helper.get_s3_object(
        s3_client=None,
        bucket=context.published_bucket,
        key=f"corporate_data_ingestion/audit_logs_transition/results/{context.correlation_id}/result.json",
    )
    if result:
        result_json = json.loads(result)
        assert int(result_json["record_ingested_count"]) == int(record_count)
    else:
        raise AssertionError("Unable to read cluster result file")


def list_objects_from_s3_with_retries(bucket, prefix, retries=3, sleep=5):
    response = []
    count = 0
    while len(response) == 0:
        response = aws_helper.get_s3_file_object_keys_matching_pattern(bucket, prefix)
        count += 1
        if count >= retries:
            break
        time.sleep(sleep)
    return response


@when("remove key '{key}' from existing file in s3 source prefix")
def step_impl(context, key):
    response = list_objects_from_s3_with_retries(
        context.corporate_storage_s3_bucket_id, context.s3_source_prefix
    )[0]
    if len(response) == 0:
        AssertionError("Unable to retrieve file from S3")
    message = aws_helper.get_s3_object(
        None, context.corporate_storage_s3_bucket_id, response
    )
    message_decompressed = gzip.decompress(message)
    message_dict = json.loads(message_decompressed)
    json_helper.remove_key_from_dict(message_dict, key)
    message_compressed = gzip.compress(str.encode(json.dumps(message_dict)))
    aws_helper.put_object_in_s3(
        message_compressed, context.corporate_storage_s3_bucket_id, response
    )


@when(
    "the value of '{key}' is replaced with '{value}' from existing file in s3 source prefix"
)
def step_impl(context, key, value):
    value = "" if value == "None" else value
    response = list_objects_from_s3_with_retries(
        context.corporate_storage_s3_bucket_id, context.s3_source_prefix
    )[0]
    if len(response) == 0:
        AssertionError("Unable to retrieve file from S3")
    message = aws_helper.get_s3_object(
        None, context.corporate_storage_s3_bucket_id, response
    )
    message_decompressed = gzip.decompress(message)
    message_dict = json.loads(message_decompressed)
    json_helper.replace_value_from_dict_using_key(message_dict, key, value)
    message_compressed = gzip.compress(str.encode(json.dumps(message_dict)))
    aws_helper.put_object_in_s3(
        message_compressed, context.corporate_storage_s3_bucket_id, response
    )


@when("invalidate JSON from existing file in s3 source prefix")
def step_impl(context):
    response = list_objects_from_s3_with_retries(
        context.corporate_storage_s3_bucket_id, context.s3_source_prefix
    )[0]
    if len(response) == 0:
        AssertionError("Unable to retrieve file from S3")
    message = aws_helper.get_s3_object(
        None, context.corporate_storage_s3_bucket_id, response
    )
    message_decompressed = gzip.decompress(message)
    message_compressed = gzip.compress(message_decompressed + str.encode("}"))
    aws_helper.put_object_in_s3(
        message_compressed, context.corporate_storage_s3_bucket_id, response
    )


@when("Hive table dumped into S3")
def step_impl(context):
    # retrieve export-date and use it as partition name
    # step should overwrite data, we should be runnable more than once
    file_name = f"{context.test_run_name}.csv"
    step_name = "automatedtests: hive-table-to-s3"
    context.results_file_key = "{}/{}".format(context.s3_destination_prefix, file_name)
    date_str = datetime.now().strftime("%Y-%m-%d")
    hive_export_bash_command = f"""
    ( 
      ( hive -e "SELECT * FROM foo.auditlog_raw where date_str='{date_str}';" > ~/{file_name} ) &&
      aws s3 cp ~/{file_name} s3://{context.published_bucket}/{context.results_file_key}
    ) &>> /var/log/dataworks-aws-corporate-data-ingestion/e2e.log
    """.replace(
        "\n", ""
    )

    step_id = emr_step_generator.generate_bash_step(
        context.corporate_data_ingestion_cluster_id,
        hive_export_bash_command,
        step_name,
    )

    step_status = aws_helper.poll_emr_cluster_step_status(
        step_id, context.corporate_data_ingestion_cluster_id, 300
    )

    if step_status != "COMPLETED":
        raise AssertionError(
            f"'{step_name}' step failed with final status of '{step_status}'"
        )


@then(
    "'{generated_record_count}' records are available in exported data from the hive table"
)
def step_impl(context, generated_record_count):
    """Match the number of records generated"""
    key = context.results_file_key
    exported_hive_data = aws_helper.get_s3_object(
        bucket=context.published_bucket,
        key=key,
        s3_client=None,
    )

    if exported_hive_data:
        exported_rows = exported_hive_data.decode().rstrip("\n").split("\n")
        num_records = len(exported_rows)
        if num_records != int(generated_record_count):
            raise AssertionError(
                f"The number of rows retrieved from Hive ({num_records})does"
                f" not match the number generated ({generated_record_count})"
            )
    else:
        raise FileNotFoundError("Couldn't retrieve results from S3")
