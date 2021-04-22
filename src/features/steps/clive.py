import json
import os

from behave import given, then, when
from helpers import (
    aws_helper,
    console_printer,
    emr_step_generator,
    file_helper,
    invoke_lambda,
)
from datetime import datetime
import operator

CLUSTER_ARN = "ClusterArn"
COMPLETED_STATUS = "COMPLETED"


@given("the results of the dynamodb table '{table_name}' for '{data_product}'")
def step_(context, table_name, data_product):
    key_dict = {
        "Correlation_Id": {"S": f"{context.test_run_name}"},
        "DataProduct": {"S": f"{data_product}"},
    }

    filters = {"DataProduct": f"{data_product}", "Status": "COMPLETED"}

    console_printer.print_info(
        f"Getting DynamoDb data with filters '{filters}' from table named '{table_name}'"
    )

    # get items from the dynamo that are completed for adg-full
    response = aws_helper.scan_dynamodb_with_filters(table_name, filters)

    # sort the items in the response by date
    response.sort(key=operator.itemgetter("Date"), reverse=True)

    latest_successfull_adg = {}
    # get the first item from the sorted list. So the latest date
    for item in response:
        if "Date" in item and "S3_Prefix_Analytical_DataSet" in item:
            latest_successfull_adg = item
            break
    console_printer.print_info(
        f"This is the response from the DynamoDB: {latest_successfull_adg}"
    )
    context.clive_test_input_s3_prefix = latest_successfull_adg[
        "S3_Prefix_Analytical_DataSet"
    ]


@then(
    "start the CLIVE cluster and wait for the step '{step_name}' for a maximum of '{timeout_mins}' minutes"
)
def step_(context, step_name, timeout_mins):
    timeout_secs = int(timeout_mins) * 60
    context.clive_export_date = datetime.now().strftime("%Y-%m-%d")
    emr_launcher_config = {
        "correlation_id": f"{context.test_run_name}",
        "s3_prefix": f"{context.clive_test_input_s3_prefix}",
    }

    payload_json = json.dumps(emr_launcher_config)
    console_printer.print_info(f"this is the payload: {payload_json}")
    cluster_response = invoke_lambda.invoke_clive_emr_launcher_lambda(payload_json)
    cluster_arn = cluster_response[CLUSTER_ARN]
    cluster_arn_arr = cluster_arn.split(":")
    cluster_identifier = cluster_arn_arr[len(cluster_arn_arr) - 1]
    cluster_identifier_arr = cluster_identifier.split("/")
    cluster_id = cluster_identifier_arr[len(cluster_identifier_arr) - 1]
    context.clive_cluster_id = cluster_id

    console_printer.print_info(f"Started emr cluster : '{cluster_id}'")
    step = aws_helper.get_emr_cluster_step(step_name, cluster_id)
    step_id = step["Id"]
    console_printer.print_info(f"Step id for '{step_name}' : '{step_id}'")
    if step is not None:
        execution_state = aws_helper.poll_emr_cluster_step_status(
            step_id, cluster_id, timeout_secs
        )
    if execution_state != COMPLETED_STATUS:
        raise AssertionError(
            f"'{step_name}' step failed with final status of '{execution_state}'"
        )


@then("insert the '{step_name}' step onto the CLIVE cluster")
def step_impl(context, step_name):
    context.clive_cluster_step_name = step_name
    s3_path = f"{context.clive_test_input_s3_prefix}/{context.test_run_name}"
    file_name = f"{context.test_run_name}.csv"
    clive_hive_export_bash_command = (
        f"hive -e 'SELECT * FROM uc_clive.contract;' >> ~/{file_name} && "
        + f"aws s3 cp ~/{file_name} s3://{context.published_bucket}/{s3_path}/"
        + f" &>> /var/log/clive/e2e.log"
    )

    context.clive_cluster_step_id = emr_step_generator.generate_bash_step(
        context.clive_cluster_id,
        clive_hive_export_bash_command,
        context.clive_cluster_step_name,
    )
    context.clive_results_s3_file = os.path.join(s3_path, file_name)


@then("wait a maximum of '{timeout_mins}' minutes for the step to finish")
def step_impl(context, timeout_mins):
    timeout_secs = int(timeout_mins) * 60
    execution_state = aws_helper.poll_emr_cluster_step_status(
        context.clive_cluster_step_id, context.clive_cluster_id, timeout_secs
    )

    if execution_state != "COMPLETED":
        raise AssertionError(
            f"'{context.clive_cluster_step_name}' step failed with final status of '{execution_state}'"
        )


@then("the CLIVE result matches the expected results of '{expected_result_file_name}'")
def step_(context, expected_result_file_name):
    console_printer.print_info(f"S3 Request Location: {context.clive_results_s3_file}")
    actual = (
        aws_helper.get_s3_object(
            None, context.published_bucket, context.clive_results_s3_file
        )
        .decode("ascii")
        .replace("\t", "")
        .replace(" ", "")
        .strip()
    )

    expected_file_name = os.path.join(
        context.fixture_path_local, "clive", "expected", expected_result_file_name,
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


@when("the CLIVE cluster tags have been created correctly")
def step_clive_cluster_tags_have_been_created_correctly(context):
    cluster_id = context.clive_cluster_id
    console_printer.print_info(f"Cluster id : {cluster_id}")
    cluster_tags = aws_helper.check_tags_of_cluster(cluster_id)
    console_printer.print_info(f"Cluster tags : {cluster_tags}")
    tags_to_check = {"Key": "Correlation_Id", "Value": context.test_run_name}
    console_printer.print_info(f"Tags to check : {tags_to_check}")
    assert tags_to_check in cluster_tags


@then("the CLIVE metadata table is correct")
def metadata_table_step_impl(context):
    data_product = f"CLIVE"
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

    allowed_steps = ["create-views-tables"]

    assert item["TimeToExist"]["N"] is not None, f"Time to exist was not set"
    assert (
        item["Run_Id"]["N"] == "1"
    ), f"Run_Id was '{item['Run_Id']['N']}', expected '1'"
    assert (
        item["Date"]["S"] == context.clive_export_date
    ), f"Date was '{item['Date']['S']}', expected '{context.clive_export_date}'"
    assert (
        item["CurrentStep"]["S"] in allowed_steps
    ), f"CurrentStep was '{item['CurrentStep']['S']}', expected one of '{allowed_steps}'"
    assert (
        item["Cluster_Id"]["S"] == context.clive_cluster_id
    ), f"Cluster_Id was '{item['Cluster_Id']['S']}', expected '{context.clive_cluster_id}'"
