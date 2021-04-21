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


# @given("the ADG uncompressed output '{input_json}' as an input data source on S3")
# def step_(context, input_json):
#     pass
# expected_file_name = os.path.join(
#     context.fixture_path_local, "pdm_data", "input_data", input_json
# )
# input_file = file_helper.get_contents_of_file(expected_file_name, False)
# inputs_s3_key = os.path.join(context.pdm_test_input_s3_prefix, input_json)
# aws_helper.put_object_in_s3(input_file, context.published_bucket, inputs_s3_key)


@given("the results of the dynamodb table '{table_name}' for '{data_product}'")
def step_(context, table_name, data_product):
    key_dict = {
        "Correlation_Id": {"S": f"{context.test_run_name}"},
        "DataProduct": {"S": f"{data_product}"},
    }

    filters = {"DataProduct": f"{data_product}", "Status": "Completed"}

    console_printer.print_info(
        f"Getting DynamoDb data with filters '{filters}' from table named '{table_name}'"
    )

    response = aws_helper.scan_dynamodb_with_filters(table_name, filters)

    console_printer.print_info(f"this is the type of it {type(response)}")

    console_printer.print_info(f"this is the full response {response}")

    response.sort(key=operator.itemgetter("Date"))

    latest_successfull_adg = ""
    for item in response:
        if "Date" and "S3_Prefix_Analytical_DataSet" in item:
            latest_successfull_adg = item
            break
    console_printer.print_info(
        f"This is the response from the DynamoDB: {latest_successfull_adg}"
    )


@when("I start the CLIVE cluster and wait for the step '{step_name}'")
def step_(context, step_name):
    context.clive_export_date = datetime.now().strftime("%Y-%m-%d")
    emr_launcher_config = {
        "correlation_id": f"{context.test_run_name}",
        "s3_prefix": "test",
    }
    payload_json = json.dumps(emr_launcher_config)
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
            step_id, cluster_id, 2500
        )
    if execution_state != COMPLETED_STATUS:
        raise AssertionError(
            f"'{step_name}' step failed with final status of '{execution_state}'"
        )


# @when("insert the '{step_name}' step onto the cluster")
# def step_impl(context, step_name):
# context.clive_cluster_step_name = step_name
# s3_path = f"{context.pdm_test_output_s3_prefix}/{context.test_run_name}"
# file_name = f"{context.test_run_name}.csv"
# pdm_hive_export_bash_command = (
#     f"hive -e 'SELECT * FROM uc.youth_obligation_details_fact_v;' >> ~/{file_name} && "
#     + f"aws s3 cp ~/{file_name} s3://{context.published_bucket}/{s3_path}/"
#     + f" &>> /var/log/pdm/e2e.log"
# )
#
# context.pdm_cluster_step_id = emr_step_generator.generate_bash_step(
#     context.pdm_cluster_id,
#     pdm_hive_export_bash_command,
#     context.pdm_cluster_step_name,
# )
# context.pdm_results_s3_file = os.path.join(s3_path, file_name)

#
# @when("wait a maximum of '{timeout_mins}' minutes for the step to finish")
# def step_impl(context, timeout_mins):
# timeout_secs = int(timeout_mins) * 60
# execution_state = aws_helper.poll_emr_cluster_step_status(
#     context.pdm_cluster_step_id, context.pdm_cluster_id, timeout_secs
# )
#
# if execution_state != "COMPLETED":
#     raise AssertionError(
#         f"'{context.pdm_cluster_step_name}' step failed with final status of '{execution_state}'"
#     )


# @then("the CLIVE result matches the expected results of '{expected_result_file_name}'")
# def step_(context, expected_result_file_name):
# console_printer.print_info(f"S3 Request Location: {context.pdm_results_s3_file}")
# actual = aws_helper.get_s3_object(
#     None, context.published_bucket, context.pdm_results_s3_file
# ).decode("ascii")
# actual_comma_deliminated = actual.replace("\t", ",").strip()
#
# expected_file_name = os.path.join(
#     context.fixture_path_local, "pdm_data", "expected", expected_result_file_name
# )
# expected = file_helper.get_contents_of_file(expected_file_name, False)
# expected_comma_deliminated = expected.replace("\t", ",").strip()
#
# assert (
#     expected_comma_deliminated == actual_comma_deliminated
# ), f"Expected result of '{expected_comma_deliminated}', does not match '{actual_comma_deliminated}'"


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
