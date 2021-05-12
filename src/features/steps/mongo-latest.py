from behave import given, then
import os
import json
import csv
from helpers import (
    aws_helper,
    invoke_lambda,
    console_printer,
    emr_step_generator,
    file_helper,
)
from datetime import datetime


@given("I start the mongo latest cluster")
def step_(context):
    context.mongo_latest_export_date = datetime.now().strftime("%Y-%m-%d")
    emr_launcher_config = {
        "correlation_id": f"{context.test_run_name}",
        "snapshot_type": "incremental",
        "s3_prefix": "NOT_SET",
        "export_date": f"{context.mongo_latest_export_date}",
    }

    payload_json = json.dumps(emr_launcher_config)
    console_printer.print_info(f"this is the payload: {payload_json}")
    cluster_response = invoke_lambda.invoke_mongo_latest_emr_launcher_lambda(
        payload_json
    )
    cluster_arn = cluster_response["ClusterArn"]
    cluster_arn_arr = cluster_arn.split(":")
    cluster_identifier = cluster_arn_arr[len(cluster_arn_arr) - 1]
    cluster_identifier_arr = cluster_identifier.split("/")
    cluster_id = cluster_identifier_arr[len(cluster_identifier_arr) - 1]
    context.mongo_latest_cluster_id = cluster_id

    console_printer.print_info(f"Started emr cluster : '{cluster_id}'")


@when("I insert the '{step_name}' step onto the mongo latest cluster")
def step_impl(context, step_name):
    context.mongo_latest_s3_prefix = os.path.join(
        context.mongo_snapshot_path, context.test_run_name
    )
    context.mongo_latest_cluster_step_name = step_name
    s3_path = f"{context.mongo_latest_s3_prefix}/{context.test_run_name}"
    file_name = f"{context.test_run_name}.csv"
    hive_export_bash_command = (
        f"hive -e 'SELECT * FROM uc_mongo_latest.statement_fact_v;' >> ~/{file_name} && "
        + f"aws s3 cp ~/{file_name} s3://{context.published_bucket}/{s3_path}/"
        + f" &>> /var/log/mongo_latest/e2e.log"
    )

    context.mongo_latest_cluster_step_id = emr_step_generator.generate_bash_step(
        context.mongo_latest_cluster_id,
        hive_export_bash_command,
        context.mongo_latest_cluster_step_name,
    )
    context.mongo_latest_results_s3_file = os.path.join(s3_path, file_name)


@when("wait a maximum of '{timeout_mins}' minutes for the mongo latest step to finish")
def step_impl(context, timeout_mins):
    timeout_secs = int(timeout_mins) * 60
    execution_state = aws_helper.poll_emr_cluster_step_status(
        context.mongo_latest_cluster_step_id,
        context.mongo_latest_cluster_id,
        timeout_secs,
    )

    if execution_state != "COMPLETED":
        raise AssertionError(
            f"'{context.mongo_latest_cluster_step_name}' step failed with final status of '{execution_state}'"
        )


@then(
    "the mongo latest result matches the expected results of '{expected_result_file_name}'"
)
def step_(context, expected_result_file_name):
    console_printer.print_info(
        f"S3 Request Location: {context.mongo_latest_results_s3_file}"
    )
    actual = (
        aws_helper.get_s3_object(
            None, context.published_bucket, context.mongo_latest_results_s3_file
        )
        .decode("ascii")
        .replace("\t", "")
        .replace(" ", "")
        .strip()
    )

    expected_file_name = os.path.join(
        context.fixture_path_local,
        "snapshot_data",
        "expected",
        expected_result_file_name,
    )
    expected = (
        file_helper.get_contents_of_file(expected_file_name, False)
        .replace("\t", "")
        .replace(" ", "")
        .strip()
    )

    assert (
        expected == actual
    ), f"Expected result of '{expected}', does not match '{actual}'"


@then("the mongo latest cluster tags have been created correctly")
def step_check_cluster_tags(context):
    console_printer.print_info(f"Checking cluster Tags")
    cluster_id = context.mongo_latest_cluster_id
    console_printer.print_info(f"Cluster id : {cluster_id}")
    cluster_tags = aws_helper.check_tags_of_cluster(cluster_id)
    console_printer.print_info(f"Cluster tags : {cluster_tags}")
    tags_to_check = {"Key": "Correlation_Id", "Value": context.test_run_name}
    console_printer.print_info(f"Tags to check : {tags_to_check}")

    assert tags_to_check in cluster_tags


@then("the mongo latest metadata table is correct")
def metadata_table_step_impl(context):
    data_product = f"MONGO_LATEST"
    table_name = "data_pipeline_metadata"

    key_dict = {
        "Correlation_Id": {"S": f"{context.test_run_name}"},
        "DataProduct": {"S": f"{data_product}"},
    }

    console_printer.print_info(
        f"Getting DynamoDb data from item with key_dict of '{key_dict}' from table named '{table_name}'"
    )

    response = aws_helper.get_item_from_dynamodb(table_name, key_dict)

    console_printer.print_info(f"Data retrieved from dynamodb table : '{response}'")

    assert (
        "Item" in response
    ), f"Could not find metadata table row with correlation id of '{context.test_run_name}' and data product  of '{data_product}'"

    item = response["Item"]
    console_printer.print_info(f"Item retrieved from dynamodb table : '{item}'")

    allowed_steps = [
        "flush-pushgateway",
        "executeUpdateAll",
        "bash",
    ]

    assert item["TimeToExist"]["N"] is not None, f"Time to exist was not set"
    assert (
        item["Run_Id"]["N"] == "1"
    ), f"Run_Id was '{item['Run_Id']['N']}', expected '1'"
    assert (
        item["Date"]["S"] == context.mongo_latest_export_date
    ), f"Date was '{item['Date']['S']}', expected '{context.mongo_latest_export_date}'"
    assert (
        item["CurrentStep"]["S"] in allowed_steps
    ), f"CurrentStep was '{item['CurrentStep']['S']}', expected one of '{allowed_steps}'"
    assert (
        item["Cluster_Id"]["S"] == context.mongo_latest_cluster_id
    ), f"Cluster_Id was '{item['Cluster_Id']['S']}', expected '{context.mongo_latest_cluster_id}'"
    assert (
        item["S3_Prefix_Snapshots"]["S"] == "NOT_SET"
    ), f"S3_Prefix_Snapshots was '{item['S3_Prefix_Snapshots']['S']}', expected 'NOT_SET'"
    assert (
        item["Snapshot_Type"]["S"] == snapshot_type
    ), f"Snapshot_Type was '{item['Snapshot_Type']['S']}', expected '{snapshot_type}'"
