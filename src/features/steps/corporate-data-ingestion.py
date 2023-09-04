import time
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
from datetime import datetime, timedelta

CLUSTER_ARN = "ClusterArn"
COMPLETED_STATUS = "COMPLETED"
CORRELATION_ID = "correlation_id"
S3_PREFIX = "s3_prefix"
EXPORT_DATE = "export_date"
SNAPSHOT_TYPE = "snapshot_type"


@given("the '{prefix_type}' prefixes are used")
def step_impl(context, prefix_type):
    if prefix_type == "audit":
        ucfs_folder = "ucfs_audit"
    else:
        ucfs_folder = "ucfs_main"

    collection_folder = "/".join(context.topics_for_test[0]["topic"].split(".")[-2:])

    context.s3_source_prefix = os.path.join(
        "corporate_storage",
        ucfs_folder,
        datetime.now().strftime("%Y/%m/%d"),
        collection_folder,
    )

    context.s3_destination_prefix = os.path.join(
        "corporate_data_ingestion/orc/daily", collection_folder
    )

    context.s3_export_destination_prefix = os.path.join(
        "corporate_data_ingestion/exports/",
        collection_folder,
        (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d"),
    )


@given("the source and destination prefixes are cleared")
def step_impl(context):
    for bucket, prefix in [
        (context.corporate_storage_s3_bucket_id, context.s3_source_prefix),
        (context.published_bucket, context.s3_destination_prefix),
    ]:
        aws_helper.clear_s3_prefix(bucket, prefix, True)


@given("Empty orc snapshot placed in S3")
def step_impl(context):
    # get expected previous snapshot prefix from dynamodb
    response = aws_helper.scan_dynamodb_with_filters(
        table_name="data_pipeline_metadata",
        filters={
            "DataProduct": "CDI-calculator:calculationParts",
            "Status": "COMPLETED",
        },
    )

    response_items = [item for item in response if "S3_Prefix_CDI_Export" in item]

    prefix = None
    if len(response_items) > 0:
        dates = [item["Date"] for item in response_items]
        dates.sort(reverse=True)
        max_date = dates[0]

        for item in response_items:
            if item["Date"] == max_date:
                prefix = item["S3_Prefix_CDI_Export"]

    prefix = (
        prefix
        if prefix
        else "corporate_data_ingestion/exports/calculator/calculationParts/2023-05-17/"
    )

    aws_helper.clear_s3_prefix(
        s3_bucket=context.published_bucket,
        path=prefix,
        delete_prefix=False,
    )

    topic = context.topics_for_test[0]["topic"]
    db, collection = topic.split(".")[-2:]

    hive_export_bash_command = f"""
    ( 
      ( hive -e "drop table if exists export_{collection}" ) &&
      ( hive -e "create temporary table export_{collection} (id string, db_type string, val string, id_part string)
      stored as orc location 's3://{context.published_bucket}/{prefix}'" ) &&
      ( hive -e "drop table if exists export_{collection}" )
    ) &>> /var/log/dataworks-aws-corporate-data-ingestion/e2e.log
    """.replace(
        "\n", ""
    )

    step_id = emr_step_generator.generate_bash_step(
        context.corporate_data_ingestion_cluster_id,
        hive_export_bash_command,
        "bash step",
    )

    step_state = aws_helper.poll_emr_cluster_step_status(
        step_id, context.corporate_data_ingestion_cluster_id, 1200
    )

    if step_state != "COMPLETED":
        raise AssertionError(f"ORC Snapshot step failed")


@given("the s3 '{type}' prefix is cleared")
@then("the s3 '{type}' prefix is cleared")
def step_impl(context, type):
    if type == "source":
        s3_source_prefix = os.path.join(
            context.s3_source_prefix,
            datetime.now().strftime("%Y/%m/%d"),
            "data/businessAudit",
        )
        aws_helper.clear_s3_prefix(
            context.corporate_storage_s3_bucket_id, s3_source_prefix, True
        )
    elif type == "destination":
        aws_helper.clear_s3_prefix(
            context.published_bucket, context.s3_destination_prefix, True
        )
    else:
        console_printer.print_error_text(
            f"Executing: clean s3 '{type}' prefix. Unknown type."
        )


@given("the s3 '{location_type}' prefix replaced by unauthorised location")
def step_impl(context, location_type):
    if location_type == "source":
        context.s3_source_prefix = "unauthorised_location/e2e"
        context.override_s3_source_prefix = context.s3_source_prefix
        aws_helper.put_object_in_s3(
            "foobar",
            context.corporate_storage_s3_bucket_id,
            os.path.join(context.s3_source_prefix, "foobar.jsonl.gz"),
        )
    elif location_type == "destination":
        context.s3_destination_prefix = "unauthorised_location/e2e"
        context.override_s3_destination_prefix = context.s3_destination_prefix
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
        s3_key=os.path.join(
            context.s3_source_prefix,
            datetime.now().strftime("%Y/%m/%d"),
            "data/businessAudit",
            filename,
        ),
    )


@when(
    "a step '{step_type}' is triggered on the EMR cluster corporate-data-ingestion with '{ingestion_class}' "
    "ingestion class with additional parameters '{additional_parameters}'"
)
def step_impl(context, step_type, ingestion_class, additional_parameters):
    cluster_state = aws_helper.poll_emr_cluster_status(
        cluster_id=context.corporate_data_ingestion_cluster_id,
        timeout_in_seconds=720,
    )

    if cluster_state != "WAITING":
        raise AssertionError("Cluster not in 'WAITING' state before timeout")

    context.step_type = step_type
    # Increments current date by one day because the step will retrieve data
    # for the previous day but test data is generated for the current day
    start_date = (datetime.now().date() + timedelta(days=1)).strftime("%Y-%m-%d")
    end_date = (datetime.now().date() + timedelta(days=1)).strftime("%Y-%m-%d")

    topic = context.topics_for_test[0]["topic"]
    db, collection = topic.split(".")[-2:]

    context.step_id = emr_step_generator.generate_spark_step(
        emr_cluster_id=context.corporate_data_ingestion_cluster_id,
        script_location="/opt/emr/steps/corporate_data_ingestion.py",
        step_type=f"""automatedtests: {step_type}""",
        command_line_arguments=f"""--correlation_id {context.test_run_name} """
        + f"""--start_date {start_date} """
        + f"""--end_date {end_date} """
        + f"""--db {db} """
        + f"""--collection {collection} """
        + f"""--concurrency 1 """
        + (
            f"""--ingestion_class {ingestion_class} """
            if ingestion_class != "None"
            else ""
        )
        + (additional_parameters if additional_parameters != "None" else ""),
    )


@then("confirm that the EMR step status is '{expected_status}'")
def step_impl(context, expected_status):
    step_status = aws_helper.poll_emr_cluster_step_status(
        context.step_id,
        context.corporate_data_ingestion_cluster_id,
        timeout_in_seconds=1200,
    )

    if step_status != expected_status:
        raise AssertionError(
            f"""automatedtests: {context.step_type} step failed with final status of '{step_status}'"""
        )


def list_objects_from_s3_with_retries(bucket, prefix, retries=10, sleep=5):
    response = []
    count = 0
    while len(response) == 0:
        response = aws_helper.get_s3_file_object_keys_matching_pattern(bucket, prefix)
        count += 1
        if count >= retries:
            raise FileNotFoundError("No s3 files matching pattern")
        time.sleep(sleep)
    return response


@when("Key '{key}' is removed from existing file in s3 source prefix")
def step_impl(context, key):
    response = list_objects_from_s3_with_retries(
        context.corporate_storage_s3_bucket_id,
        context.s3_source_prefix,
    )

    object_key = response[0]

    message = aws_helper.get_s3_object(
        None, context.corporate_storage_s3_bucket_id, object_key
    )
    message_decompressed = bytes(gzip.decompress(message)).decode()
    message_dicts = [
        json.loads(message) for message in message_decompressed.split("\n") if message
    ]

    for message in message_dicts:
        json_helper.remove_key_from_dict(message, key)

    altered_message_lines = [json.dumps(message) for message in message_dicts]
    compressed_altered_messages = gzip.compress(
        str.encode("\n".join(altered_message_lines))
    )

    aws_helper.put_object_in_s3(
        compressed_altered_messages, context.corporate_storage_s3_bucket_id, object_key
    )


@when(
    "the value of '{key}' is replaced with '{value}' from existing file in s3 source prefix"
)
def step_impl(context, key, value):
    time.sleep(15)
    value = "" if value == "None" else value
    response = list_objects_from_s3_with_retries(
        context.corporate_storage_s3_bucket_id,
        context.s3_source_prefix,
    )
    object_key = response[0]

    message = aws_helper.get_s3_object(
        None, context.corporate_storage_s3_bucket_id, object_key
    )
    message_decompressed = bytes(gzip.decompress(message)).decode()
    message_dicts = [
        json.loads(message) for message in message_decompressed.split("\n") if message
    ]

    for message in message_dicts:
        json_helper.replace_value_from_dict_using_key(message, key, value)

    altered_message_lines = [json.dumps(message) for message in message_dicts]
    compressed_altered_messages = gzip.compress(
        str.encode("\n".join(altered_message_lines))
    )

    aws_helper.put_object_in_s3(
        compressed_altered_messages, context.corporate_storage_s3_bucket_id, object_key
    )


@when("invalidate JSON from existing file in s3 source prefix")
def step_impl(context):
    time.sleep(10)
    response = list_objects_from_s3_with_retries(
        context.corporate_storage_s3_bucket_id,
        context.s3_source_prefix,
    )

    object_key = response[0]

    message = aws_helper.get_s3_object(
        None, context.corporate_storage_s3_bucket_id, object_key
    )

    message_decompressed = gzip.decompress(message)
    message_compressed = gzip.compress(message_decompressed + str.encode("}"))
    aws_helper.put_object_in_s3(
        message_compressed, context.corporate_storage_s3_bucket_id, object_key
    )


@when("Daily Data is dumped to S3")
def step_impl(context):
    file_name = f"{context.test_run_name}.csv"
    step_name = "automatedtests: check-daily-data"
    context.results_file_key = os.path.join(
        *context.s3_destination_prefix.split("/")[:-1], file_name
    )

    topic = context.topics_for_test[0]["topic"]
    db, collection = topic.split(".")[-2:]

    export_location = os.path.join(
        "s3://", context.published_bucket, context.s3_destination_prefix
    )
    output_file = os.path.join(
        "s3://", context.published_bucket, context.results_file_key
    )
    hive_export_bash_command = f"""
    ( 
      ( hive -e "drop table if exists export_table_{collection}" ) &&
      ( hive -e "create external table export_table_{collection} (val string) stored as ORC location '{export_location}'" ) &&
      ( hive -e "SELECT * FROM export_table_{collection};" > ~/{file_name} ) &&
      ( aws s3 cp ~/{file_name} {output_file} )
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


@when("Export Data is dumped to S3")
def step_impl(context):
    file_name = f"{context.test_run_name}.csv"
    step_name = "automatedtests: check-daily-data"
    context.results_file_key = os.path.join(
        *context.s3_export_destination_prefix.split("/")[:-1], file_name
    )

    topic = context.topics_for_test[0]["topic"]
    db, collection = topic.split(".")[-2:]

    export_location = os.path.join(
        "s3://", context.published_bucket, context.s3_export_destination_prefix
    )
    output_file = os.path.join(
        "s3://", context.published_bucket, context.results_file_key
    )
    hive_export_bash_command = f"""
    ( 
      ( hive -e "drop table if exists export_table_{collection}" ) &&
      ( hive -e "create external table export_table_{collection} (val string) stored as ORC location '{export_location}'" ) &&
      ( hive -e "SELECT * FROM export_table_{collection};" > ~/{file_name} ) &&
      ( aws s3 cp ~/{file_name} {output_file} )
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


@when("The audit hive table is dumped into S3")
def step_impl(context):
    # retrieve export-date and use it as partition name
    # step should overwrite data, we should be runnable more than once
    file_name = f"{context.test_run_name}.csv"
    step_name = "automatedtests: hive-table-to-s3"
    context.results_file_key = os.path.join(
        *context.s3_destination_prefix.split("/")[:-1], file_name
    )
    date_str = (datetime.now().date() + timedelta(days=1)).strftime("%Y-%m-%d")
    hive_export_bash_command = f"""
    ( 
      ( hive -e "SELECT * FROM uc_dw_auditlog.auditlog_raw where date_str='{date_str}';" > ~/{file_name} ) &&
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
