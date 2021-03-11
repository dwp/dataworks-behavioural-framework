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

CLUSTER_ARN = "ClusterArn"
COMPLETED_STATUS = "COMPLETED"


@given("the ADG uncompressed output '{input_json}' as an input data source on S3")
def step_(context, input_json):
    expected_file_name = os.path.join(
        context.fixture_path_local, "pdm_data", "input_data", input_json
    )
    input_file = file_helper.get_contents_of_file(expected_file_name, False)
    inputs_s3_key = os.path.join(context.pdm_test_input_s3_prefix, input_json)
    aws_helper.put_object_in_s3(input_file, context.published_bucket, inputs_s3_key)


@when("I start the PDM cluster")
def step_(context):
    emr_launcher_config = {
        "s3_prefix": context.pdm_test_input_s3_prefix,
        "correlation_id": f"{context.test_run_name}",
    }
    payload_json = json.dumps(emr_launcher_config)
    cluster_response = invoke_lambda.invoke_pdm_emr_launcher_lambda(payload_json)
    cluster_arn = cluster_response[CLUSTER_ARN]
    cluster_arn_arr = cluster_arn.split(":")
    cluster_identifier = cluster_arn_arr[len(cluster_arn_arr) - 1]
    cluster_identifier_arr = cluster_identifier.split("/")
    cluster_id = cluster_identifier_arr[len(cluster_identifier_arr) - 1]
    context.pdm_cluster_id = cluster_id

    console_printer.print_info(f"Started emr cluster : '{cluster_id}'")


@when("insert the '{step_name}' step onto the cluster")
def step_impl(context, step_name):
    context.pdm_cluster_step_name = step_name
    s3_path = f"{context.pdm_test_output_s3_prefix}/{context.test_run_name}"
    file_name = f"{context.test_run_name}.csv"
    pdm_hive_export_bash_command = (
        f"hive -e 'SELECT * FROM uc.youth_obligation_details_fact_v;' >> ~/{file_name} && "
        + f"aws s3 cp ~/{file_name} s3://{context.published_bucket}/{s3_path}/"
        + f" &>> /var/log/pdm/e2e.log"
    )

    context.pdm_cluster_step_id = emr_step_generator.generate_bash_step(
        context.pdm_cluster_id,
        pdm_hive_export_bash_command,
        context.pdm_cluster_step_name,
    )
    context.pdm_results_s3_file = os.path.join(s3_path, file_name)


@when("wait a maximum of '{timeout_mins}' minutes for the step to finish")
def step_impl(context, timeout_mins):
    timeout_secs = int(timeout_mins) * 60
    execution_state = aws_helper.poll_emr_cluster_step_status(
        context.pdm_cluster_step_id, context.pdm_cluster_id, timeout_secs
    )

    if execution_state != "COMPLETED":
        raise AssertionError(
            f"'{context.pdm_cluster_step_name}' step failed with final status of '{execution_state}'"
        )


@then("the PDM result matches the expected results of '{expected_result_file_name}'")
def step_(context, expected_result_file_name):
    console_printer.print_info(f"S3 Request Location: {context.pdm_results_s3_file}")
    actual = aws_helper.get_s3_object(
        None, context.published_bucket, context.pdm_results_s3_file
    ).decode("ascii")
    actual_comma_deliminated = actual.replace("\t", ",").strip()

    expected_file_name = os.path.join(
        context.fixture_path_local, "pdm_data", "expected", expected_result_file_name
    )
    expected = file_helper.get_contents_of_file(expected_file_name, False)
    expected_comma_deliminated = expected.replace("\t", ",").strip()

    assert (
        expected_comma_deliminated == actual_comma_deliminated
    ), f"Expected result of '{expected_comma_deliminated}', does not match '{actual_comma_deliminated}'"


@then("the PDM cluster tags have been created correctly")
def step_pdm_cluster_tags_have_been_created_correctly(context):

    cluster_id = context.pdm_cluster_id

    console_printer.print_info(f"Cluster id : {cluster_id}")

    cluster_tags = aws_helper.check_tags_of_cluster(cluster_id)

    tags_to_check = {'Key': 'Correlation_Id', 'Value': context.test_run_name}

    assert tags_to_check in cluster_tags
