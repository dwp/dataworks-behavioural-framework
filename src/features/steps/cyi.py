import json
import os
import time
import gzip
import shutil

from behave import given, then, when
from helpers import (
    aws_helper,
    console_printer,
    emr_step_generator,
    file_helper,
    invoke_lambda,
)

from datetime import datetime


@given("I upload a CYI file")
def step_impl(context):
    context.cyi_export_date = datetime.now().strftime("%Y-%m-%d")
    input_file_unzipped = os.path.join(
        context.fixture_path_local, "cyi", "input", "cyi_input.json"
    )
    input_file_zipped = os.path.join(context.temp_folder, "cyi_input.json.gz")
    s3_prefix = os.path.join(
        context.cyi_input_s3_prefix, context.cyi_export_date, "cyi_input.json.gz"
    )

    with open(input_file_unzipped, "rb") as f_in:
        with gzip.open(input_file_zipped, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    aws_helper.upload_file_to_s3_and_wait_for_consistency(
        input_file_zipped,
        context.s3_ingest_bucket,
        context.timeout,
        s3_prefix,
    )


@given("I wait '{timeout_minutes}' minutes for a CYI cluster to start")
def step_impl(context, timeout_minutes):
    context.cyi_cluster_id = None

    timeout_time = time.time() + (int(timeout_minutes) * 60)

    time_string = time.strftime("%b %d %Y %H:%M:%S", time.localtime(timeout_time))
    console_printer.print_info(
        f"Waiting until '{time_string}' for bootstrapping or running CYI cluster"
    )

    while context.cyi_cluster_id == None and time.time() < timeout_time:
        context.cyi_cluster_id = aws_helper.get_newest_emr_cluster_id(
            "cyi", ["BOOTSTRAPPING", "RUNNING"]
        )
        if context.cyi_cluster_id is not None:
            console_printer.print_info(
                f"Found cluster with id of '{context.cyi_cluster_id}'"
            )
        time.sleep(5)

    assert (
        context.cyi_cluster_id is not None
    ), f"Could not find a running CYI cluster after {timeout_minutes} minutes"


@when("I insert the '{step_name}' step onto the CYI cluster")
def step_impl(context, step_name):
    context.cyi_cluster_step_name = step_name
    s3_path = f"{context.cyi_test_input_s3_prefix}/{context.test_run_name}"
    file_name = f"{context.test_run_name}.csv"
    cyi_hive_export_bash_command = (
        f"hive -e 'SELECT * FROM cyi.cyi_managed;' >> ~/{file_name} && "
        + f"aws s3 cp ~/{file_name} s3://{context.published_bucket}/{s3_path}/"
        + f" &>> /var/log/cyi/e2e.log"
    )

    context.cyi_cluster_step_id = emr_step_generator.generate_bash_step(
        context.cyi_cluster_id,
        cyi_hive_export_bash_command,
        context.cyi_cluster_step_name,
    )
    context.cyi_results_s3_file = os.path.join(s3_path, file_name)


@when("I get the CYI Correlation Id from the tags")
def step_impl(context):
    cluster_tags = aws_helper.check_tags_of_cluster(context.cyi_cluster_id)

    console_printer.print_info(
        f"Retrieved tags of '{cluster_tags}' from cluster with id of '{context.cyi_cluster_id}'"
    )

    context.cyi_cluster_correlation_id = None
    for cluster_tag in cluster_tags:
        if cluster_tag["Key"] == "Correlation_Id":
            context.cyi_cluster_correlation_id = cluster_tag["Value"]

    assert (
        context.cyi_cluster_correlation_id is not None
    ), f"Could not find correlation id in cluster tags of '{cluster_tags}'"

    console_printer.print_info(
        f"Retrieved Correlation_Id of '{context.cyi_cluster_correlation_id}'"
    )


@when("I wait '{timeout_mins}' minutes for the CYI hive-query to finish")
def step_impl(context, timeout_mins):
    timeout_secs = int(timeout_mins) * 60
    execution_state = aws_helper.poll_emr_cluster_step_status(
        context.cyi_cluster_step_id, context.cyi_cluster_id, timeout_secs
    )

    assert (
        execution_state == "COMPLETED"
    ), f"'{context.cyi_cluster_step_name}' step failed with final status of '{execution_state}'"


@then("the CYI result matches the expected results of '{expected_result_file_name}'")
def step_(context, expected_result_file_name):
    console_printer.print_info(f"S3 Request Location: {context.cyi_results_s3_file}")
    actual = (
        aws_helper.get_s3_object(
            None, context.published_bucket, context.cyi_results_s3_file
        )
        .decode("ascii")
        .replace("\t", "")
        .replace(" ", "")
        .strip()
    )

    expected_file_name = os.path.join(
        context.fixture_path_local,
        "cyi",
        "expected",
        expected_result_file_name,
    )
    expected = (
        file_helper.get_contents_of_file(expected_file_name, False)
        .replace("export_date", context.cyi_export_date)
        .replace("\t", "")
        .replace(" ", "")
        .strip()
    )

    assert (
        expected in actual
    ), f"Expected result of '{expected}', does not match '{actual}'"


@then("I check the CYI metadata table is correct")
def metadata_table_step_impl(context):
    data_product = "CYI"
    table_name = "data_pipeline_metadata"

    key_dict = {
        "Correlation_Id": {"S": f"{context.cyi_cluster_correlation_id}"},
        "DataProduct": {"S": f"{data_product}"},
    }

    console_printer.print_info(
        f"Getting DynamoDb data from item with key_dict of '{key_dict}' from table named '{table_name}'"
    )

    response = aws_helper.get_item_from_dynamodb(table_name, key_dict)

    console_printer.print_info(f"Data retrieved from dynamodb table : '{response}'")

    assert (
        "Item" in response
    ), f"Could not find metadata table row with correlation id of '{context.cyi_cluster_correlation_id}' and data product  of '{data_product}'"

    item = response["Item"]
    console_printer.print_info(f"Item retrieved from dynamodb table : '{item}'")

    allowed_steps = [
        "run-cyi",
        "hive-query",
    ]

    assert item["TimeToExist"]["N"] is not None, f"Time to exist was not set"
    assert (
        item["Run_Id"]["N"] == "1"
    ), f"Run_Id was '{item['Run_Id']['N']}', expected '1'"
    assert (
        item["Date"]["S"] == context.cyi_export_date
    ), f"Date was '{item['Date']['S']}', expected '{context.cyi_export_date}'"
    assert (
        item["CurrentStep"]["S"] in allowed_steps
    ), f"CurrentStep was '{item['CurrentStep']['S']}', expected one of '{allowed_steps}'"
    assert (
        item["Cluster_Id"]["S"] == context.cyi_cluster_id
    ), f"Cluster_Id was '{item['Cluster_Id']['S']}', expected '{context.cyi_cluster_id}'"
