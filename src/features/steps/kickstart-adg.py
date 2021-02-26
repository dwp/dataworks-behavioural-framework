from behave import given, when, then
import os
import json
from datetime import datetime, timedelta
from helpers import (
    emr_step_generator,
    aws_helper,
    invoke_lambda,
    console_printer,
    file_helper,
    kickstart_adg_helper,
)

S3_KEY_KICSKTART_TEST = "kickstart-e2e-tests"
COMPLETED_STATUS = "COMPLETED"
CLUSTER_ARN = "ClusterArn"
AUDIT_TABLE_HASH_KEY = "Correlation_Id"
AUDIT_TABLE_RANGE_KEY = "DataProduct"
DYNAMO_DB_TABLE_NAME = "data_pipeline_metadata"
RUNNING_STATUS = "RUNNING"

@given(
    "The template file '{template_name}' as an input, generate '{record_count}' records per table for '{module_name}' with PII flag is '{PII_Flag}' and upload to s3 bucket"
)
def step_impl(context, template_name, record_count, module_name, PII_Flag):

    console_printer.print_info(
        f"Extracting the file properties from {template_name} for module {module_name}"
    )
    context.kickstart_schema_config = kickstart_adg_helper.get_schema_config(
        context.fixture_path_local, template_name
    )[module_name]

    console_printer.print_info(
        f"generating the input datasets locally with {record_count} records per file for given config \n"
        f"{json.dumps(context.kickstart_schema_config)}"
    )
    list_of_local_files = kickstart_adg_helper.generate_data(
        module_name, record_count, context.kickstart_schema_config, context.temp_folder
    )

    console_printer.print_info(
        f"Adding the list of files generated for current e2e test run into context for validation steps. The list of files are \n"
        f"{list_of_local_files}"
    )
    context.kickstart_current_run_input_files = list_of_local_files

    if PII_Flag.lower() == "false":
        console_printer.print_info(
            f"PII_flg set {PII_Flag}. Hence while will be upload directly from s3 bucket without record level encryption"
        )
        for file in list_of_local_files:
            console_printer.print_info(f"The file name is {file}")
            file_name = os.path.basename(file)
            input_file = file_helper.get_contents_of_file(file, False)
            inputs_s3_key = os.path.join(S3_KEY_KICSKTART_TEST, file_name)
            console_printer.print_info(
                f"Uploading the local file {file} with basename as {file_name} into s3 bucket {context.published_bucket} using key name as {inputs_s3_key}"
            )
            aws_helper.put_object_in_s3(
                input_file, context.published_bucket, inputs_s3_key
            )


@then(
    "Start kickstart adg emr process for module '{module_name}' and wait for step '{step_name}' to run"
)
def step_impl(context, module_name, step_name):

    correlation_id = f"kickstart_{module_name}_analytical_dataset_generation"
    data_product_name = "KICKSTART-ADG"
    processing_dt = datetime.strftime(datetime.now() - timedelta(days=1), "%Y-%m-%d")
    status = "COMPLETED"

    console_printer.print_info(
        f"The value to used as run time parameter \n"
        + f"correlation_id = {correlation_id} \n"
        + f"data_product_name = {data_product_name} \n"
        + f"processing_dt = {processing_dt} \n"
        + f"status  = {status} \n"
    )

    console_printer.print_info(
        f"Adjusting entry in dynamodb table {DYNAMO_DB_TABLE_NAME} for correlation_id {correlation_id} for e2e test"
    )

    Item = {
        AUDIT_TABLE_HASH_KEY: {"S": correlation_id},
        AUDIT_TABLE_RANGE_KEY: {"S": data_product_name},
        "Date": {"S": processing_dt},
        "Run_Id": {"N": "1"},
        "Status": {"S": status},
    }

    aws_helper.insert_item_to_dynamo_db(DYNAMO_DB_TABLE_NAME, Item)

    console_printer.print_info(
        f"Launching the kickstart adg emr cluster for {module_name}"
    )

    emr_launcher_config = {
        "additional_step_args": {
            "submit-job": [
                "--correlation_id",
                correlation_id,
                "--job_name",
                "kickstart",
                "--module_name",
                f"{module_name}",
                "--e2e_test_flg",
                "True",
            ]
        }
    }
    payload_json = json.dumps(emr_launcher_config)
    cluster_response = invoke_lambda.invoke_kickstart_adg_emr_launcher_lambda(
        payload_json
    )
    cluster_arn = cluster_response[CLUSTER_ARN]
    cluster_arn_arr = cluster_arn.split(":")
    cluster_identifier = cluster_arn_arr[len(cluster_arn_arr) - 1]
    cluster_identifier_arr = cluster_identifier.split("/")
    cluster_id = cluster_identifier_arr[len(cluster_identifier_arr) - 1]
    context.kickstart_adg_cluster_id = cluster_id
    console_printer.print_info(f"Started emr cluster : '{cluster_id}'")
    step = aws_helper.get_emr_cluster_step(step_name, cluster_id)
    context.kickstart_step_id = step["Id"]
    console_printer.print_info(f"Step id for '{step_name}' : '{context.kickstart_step_id}'")
    if step is not None:
        execution_state = aws_helper.poll_emr_cluster_step_status(
            context.kickstart_step_id, cluster_id, 1200
        )
        if execution_state != RUNNING_STATUS:
            raise AssertionError(
                f"'{step_name}' step failed with final status of '{execution_state}'"
            )

@then(
    "Add steps '{step_name}' to kickstart adg emr cluster for '{module_name}' and wait for all steps to be completed"
)
def step_impl(context, step_name, module_name):

    context.kickstart_adg_hive_cluster_step_name = step_name
    context.kickstart_hive_result_path = f"{S3_KEY_KICSKTART_TEST}"

    console_printer.print_info(f"generating the list of hive queries to be executed")

    hive_queries_list = kickstart_adg_helper.generate_hive_queries(
        context.kickstart_schema_config,
        context.published_bucket,
        context.kickstart_hive_result_path,
    )

    console_printer.print_info(
        f"add hive queries as step to kickstart adg EMR cluster to get end result"
    )

    hive_query_step_lst = []

    for hive_query in hive_queries_list:
        kickstart_hive_query_step_id = emr_step_generator.generate_bash_step(
            context.kickstart_adg_cluster_id,
            hive_query,
            context.kickstart_adg_hive_cluster_step_name,
        )
        hive_query_step_lst.append(kickstart_hive_query_step_id)

    console_printer.print_info(
        f"check if spark step with {context.kickstart_step_id} is complete or not"
    )
    execution_state = aws_helper.poll_emr_cluster_step_status(
        context.kickstart_step_id, context.kickstart_adg_cluster_id, 1200
    )
    if execution_state != COMPLETED_STATUS:
        raise AssertionError(
            f"spark-submit step with step Id {context.kickstart_step_id} failed with final status of '{execution_state}'"
        )

    for hive_query_step in hive_query_step_lst:
        console_printer.print_info(
            f"check if hive validation queries step with {hive_query_step} is complete or not"
        )
        execution_state = aws_helper.poll_emr_cluster_step_status(
            context.hive_query_step, context.kickstart_adg_cluster_id, 1200
        )
        if execution_state != COMPLETED_STATUS:
            raise AssertionError(
                f"'{step_name}' step with step Id {context.kickstart_adg_cluster_id} failed with final status of '{execution_state}'"
            )


@then("The input result matches with final output for module '{module_name}'")
def step_(context, module_name):
    for collection in context.kickstart_schema_config["schema"].keys():
        s3_result_key = os.path.join(
            context.kickstart_hive_result_path, f"e2e_{collection}.csv"
        )
        console_printer.print_info(f"S3 Request Location: {s3_result_key}")
        actual = aws_helper.get_s3_object(
            None, context.published_bucket, s3_result_key
        ).decode("utf-8")
        actual_comma_deliminated = actual.replace("\t", ",").strip().splitlines()
        expected_file_name = [
            file
            for file in context.kickstart_current_run_input_files
            if collection in file
        ][0]
        console_printer.print_info(f"Expected File Name: {expected_file_name}")
        expected = file_helper.get_contents_of_file(
            expected_file_name, False
        ).splitlines()[1:]

        for input_line, output_line in zip(actual_comma_deliminated, expected):

            assert (
                input_line.lower() == output_line.lower()
            ), f"Expected result of '{input_line}', does not match '{output_line}' for collection {collection}"
