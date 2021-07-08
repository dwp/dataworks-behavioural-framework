import json
import re
import os
from uuid import uuid4
import time
from behave import given, when, then

from helpers import (
    invoke_lambda,
    console_printer,
    aws_helper,
    emr_step_generator,
)

CORRELATION_ID = "correlation_id"
S3_PREFIX = "s3_prefix"
CLUSTER_ARN = "ClusterArn"
COMPLETED_STATUS = "COMPLETED"


@given("Data added to hbase is flushed to S3")
def step_impl(context):
    # context dependencies
    collections = [
        (topic["topic"].replace("db.", "").replace(".", ":"))
        for topic in context.topics_for_test
    ]
    ingest_hbase_id = context.ingest_hbase_emr_cluster_id

    # generate command(s) to flush data
    command_list = [f"flush '{collection}'" for collection in collections]
    command_string = 'hbase shell <<< "' + "; ".join(command_list) + '"'

    # execute commands using bash step, and wait
    step_name = "flush tables"
    step = emr_step_generator.generate_bash_step(
        emr_cluster_id=ingest_hbase_id, bash_command=command_string, step_type=step_name
    )
    execution_state = aws_helper.poll_emr_cluster_step_status(
        step, ingest_hbase_id, 2500
    )
    if execution_state != COMPLETED_STATUS:
        raise AssertionError(
            f"'{step_name}' step failed with final status of '{execution_state}'"
        )


@given("The S3 directory is cleared")
def step_impl(context):
    bucket = context.published_bucket
    folder = context.ingest_replica_output_s3_prefix + "/"
    aws_helper.clear_s3_prefix(bucket, folder, False)


@given("The incremental ingest-replica cluster is launched with no steps")
def step_impl(context):
    emr_launcher_config = {
        "s3_overrides": None,
        "overrides": {
            "Name": "ingest-replica-incremental-e2e",
            "Instances": {"KeepJobFlowAliveWhenNoSteps": True},
            "Steps": []},
        "extend": None,
        "additional_step_args": None,
    }
    payload_json = json.dumps(emr_launcher_config)
    cluster_response = (
        invoke_lambda.invoke_ingest_replica_incremental_emr_launcher_lambda(
            payload_json
        )
    )

    cluster_arn = cluster_response[CLUSTER_ARN]
    cluster_arn_arr = cluster_arn.split(":")
    cluster_identifier = cluster_arn_arr[len(cluster_arn_arr) - 1]
    cluster_identifier_arr = cluster_identifier.split("/")
    cluster_id = cluster_identifier_arr[len(cluster_identifier_arr) - 1]
    context.ingest_replica_emr_cluster_id = cluster_id
    console_printer.print_info(f"Started emr cluster : '{cluster_id}'")


@given("dynamodb is prepared with collection details")
def step_impl(context):
    # context dependencies
    collections_list = [
        (topic["topic"].replace("db.", "").replace(".", ":"))
        for topic in context.topics_for_test
    ]

    # Get relevant database items
    database_items = []
    for collection in collections_list:
        database_items += aws_helper.scan_dynamodb_with_filters(
            "intra-day", {"Collection": collection}
        )

    # Delete relevant database items
    for item in database_items:
        aws_helper.delete_item_from_dynamodb(
            "intra-day",
            {
                "CorrelationId": {"S": item["CorrelationId"]},
                "Collection": {"S": item["Collection"]},
            },
        )

    # Create initial records for collections
    for collection in collections_list:
        aws_helper.insert_item_to_dynamo_db(
            table_name="intra-day",
            item_dict={
                "CorrelationId": {"S": "000"},
                "Collection": {"S": collection},
                "TriggeredTime": {"N": "0"},
                "ProcessedDataEnd": {"N": "0"},
                "JobStatus": {"S": "EMR_COMPLETED"},
            },
        )


@when("The pyspark step is added to the ingest-replica cluster")
def step_impl(context):
    # context dependencies
    ingest_replica_id = context.ingest_replica_emr_cluster_id
    collections_spaced = " ".join(
        [
            (topic["topic"].replace("db.", "").replace(".", ":"))
            for topic in context.topics_for_test
        ]
    )
    output_s3_bucket = context.published_bucket
    output_s3_prefix = context.ingest_replica_output_s3_prefix
    context.database_name = "intra_day_tests"

    # Add step to cluster
    command = " ".join(
        [
            "spark-submit /var/ci/generate_dataset_from_hbase.py",
            f"scheduled",
            f"--correlation_id {uuid4()}",
            f"--collections {collections_spaced}",
            f"--output_s3_bucket {output_s3_bucket}",
            f"--output_s3_prefix {output_s3_prefix}",
            f"--triggered_time {int(time.time()*1000)}",
            f"--database_name {context.database_name}",
            f"--end_time {int(time.time()*1000)}",
        ]
    )

    step_name = "spark-submit"
    step = emr_step_generator.generate_bash_step(
        emr_cluster_id=ingest_replica_id, bash_command=command, step_type=step_name
    )
    execution_state = aws_helper.poll_emr_cluster_step_status(
        step, ingest_replica_id, 2500
    )
    if execution_state != COMPLETED_STATUS:
        raise AssertionError(
            f"'{step_name}' step failed with final status of '{execution_state}'"
        )


@when("Hive verification step is added to the cluster")
def step_impl(context):
    # context dependencies
    collections = [collection["topic"] for collection in context.topics_for_test]
    published_bucket = context.published_bucket
    output_s3_prefix = context.ingest_replica_output_s3_prefix
    database_name = context.database_name

    # context outputs
    context.hive_s3_outputs = []
    context.hive_steps = []

    for collection in collections:
        hbase_name = collection.replace("db.", "").replace(".", ":")
        hive_name = hbase_name.replace(":", "_")
        s3_file = "s3://{bucket}/{prefix}/{folder}/{file}".format(
            bucket=published_bucket,
            prefix=output_s3_prefix,
            folder="query_output",
            file=hive_name,
        )
        hive_export_bash_command = (
            f"hive -e 'Select * from {database_name}.{hive_name};' >> ~/{hive_name} && "
            f"aws s3 cp ~/{hive_name} {s3_file}"
            f" &>> /var/log/e2e.log"
        )

        context.hive_steps.append(
            emr_step_generator.generate_bash_step(
                context.ingest_replica_emr_cluster_id,
                hive_export_bash_command,
                hive_name,
            )
        )

        context.hive_s3_outputs.append((collection, s3_file))

    for step in context.hive_steps:
        console_printer.print_info(f"Step id for Hive query : '{step}'")
        execution_state = aws_helper.poll_emr_cluster_step_status(
            step, context.ingest_replica_emr_cluster_id, 2500
        )
        if execution_state != COMPLETED_STATUS:
            raise AssertionError(
                f"Hive verification step id:{step} failed with final status of '{execution_state}'"
            )


@then("The processed data is available in HIVE")
def step_impl(context):
    # Generated IDs
    generated_ids = {}
    for collection in context.topics_for_test:
        generated_ids[collection["topic"]] = set()
        for file_name in os.listdir(
            os.path.join(
                context.snapshot_files_hbase_records_temp_folder, collection["topic"]
            )
        ):
            file_path = os.path.join(
                context.snapshot_files_hbase_records_temp_folder,
                collection["topic"],
                file_name,
            )
            with open(file_path, "rb") as file:
                generated_ids[collection["topic"]].add(
                    json.loads((file.read().decode("ascii")))["_id"]["e2eId"]
                )

    # Hive IDs
    hive_ids = {}
    for collection, file in context.hive_s3_outputs:
        match = re.match(r"s3://(?P<bucket>[a-zA-Z0-9\-.]{3,63})/(?P<path>.*)", file)
        if match:
            bucket = match["bucket"]
            path = match["path"]
            file_content = (
                aws_helper.get_s3_object(
                    None,
                    bucket,
                    path,
                )
                .decode("ascii")
                .replace(" ", "")
                .strip()
            )

            hive_records = [
                json.loads(line.split("\t")[2]) for line in file_content.split("\n")
            ]

            hive_ids[collection] = {record["_id"]["e2eId"] for record in hive_records}
        else:
            raise AssertionError(f"S3 file {file} not parseable")

    for collection_name, id_set in generated_ids.items():
        assert id_set == hive_ids[collection_name]
