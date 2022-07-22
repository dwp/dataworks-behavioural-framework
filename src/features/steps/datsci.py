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
import operator

CLUSTER_ARN = "ClusterArn"
COMPLETED_STATUS = "COMPLETED"


@given("I start the DATSCI cluster")
def step_(context):
    emr_launcher_config = {
        "s3_overrides": None,
        "overrides": None,
        "extend": None,
        "additional_step_args": None,
    }

    payload_json = json.dumps(emr_launcher_config)
    console_printer.print_info(f"this is the payload: {payload_json}")
    cluster_response = invoke_lambda.invoke_ap_launcher_lambda(payload_json)
    print(cluster_response)
    cluster_arn = cluster_response[CLUSTER_ARN]
    cluster_arn_arr = cluster_arn.split(":")
    cluster_identifier = cluster_arn_arr[len(cluster_arn_arr) - 1]
    cluster_identifier_arr = cluster_identifier.split("/")
    cluster_id = cluster_identifier_arr[len(cluster_identifier_arr) - 1]
    context.datsci_cluster_id = cluster_id

    console_printer.print_info(f"Started emr cluster : '{cluster_id}'")


@then("I insert the '{step_name}' step onto the DATSCI cluster")
def step_impl(context, step_name):
    context.datsci_cluster_step_name = step_name
    datsci_bash_command = (
        f"hive -e 'SELECT * FROM site_data_csv;'"
    )

    context.datsci_cluster_step_id = emr_step_generator.generate_bash_step(
        context.datsci_cluster_id,
        datsci_bash_command,
        context.datsci_cluster_step_name,
    )


@then("I wait '{timeout_mins}' minutes")
def step_impl(context, timeout_mins):
    timeout_secs = int(timeout_mins) * 60
    execution_state = aws_helper.poll_emr_cluster_step_status(
        context.datsci_cluster_step_id, context.datsci_cluster_id, timeout_secs
    )

    print(execution_state)

    if execution_state != "COMPLETED":
        raise AssertionError(
            f"'{context.datsci_cluster_step_name}' step failed with final status of '{execution_state}'"
        )


@then("I check that the DATSCI cluster tags have been created correctly")
def step_datsci_cluster_tags_have_been_created_correctly(context):
    cluster_id = context.datsci_cluster_id
    console_printer.print_info(f"Cluster id : {cluster_id}")
    cluster_tags = aws_helper.check_tags_of_cluster(cluster_id)
    console_printer.print_info(f"Cluster tags : {cluster_tags}")
    tags_to_check = {"Key": "Application", "Value": "datsci-model-build"}
    console_printer.print_info(f"Tags to check : {tags_to_check}")
    assert tags_to_check in cluster_tags