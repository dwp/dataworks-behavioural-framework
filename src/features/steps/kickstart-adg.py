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


@given("The template file '{template_name}' as an input")
def step_impl(context, template_name):
    context.kickstart_current_run_input_files = []
    console_printer.print_info(f"Extracting the file properties from {template_name}")
    context.kickstart_schema_config = kickstart_adg_helper.get_schema_config(
        context.fixture_path_local, template_name
    )


@given(
    "Generate '{record_count}' records per table for '{module_name}' with PII flag as '{PII_Flag}' and upload to s3 bucket"
)
def step_impl(context, record_count, module_name, PII_Flag):
    schema_config = context.kickstart_schema_config[module_name]
    console_printer.print_info(
        f"generating the input datasets locally with {record_count} records per file for given config \n"
        f"{json.dumps(schema_config)}"
    )
    list_of_local_files = kickstart_adg_helper.generate_data(
        module_name, record_count, schema_config, context.temp_folder
    )

    console_printer.print_info(
        f"Adding the list of files generated for current e2e test run into context for validation steps. The list of files are \n"
        f"{list_of_local_files}"
    )
    if context.kickstart_current_run_input_files:
        context.kickstart_current_run_input_files.extend(list_of_local_files)
    else:
        context.kickstart_current_run_input_files = list_of_local_files

    if PII_Flag.lower() == "false":
        console_printer.print_info(
            f"PII_flg set {PII_Flag}. Hence while will be upload directly from s3 bucket without record level encryption"
        )
        kickstart_adg_helper.files_upload_to_s3(
            context,
            list_of_local_files,
            folder_name=S3_KEY_KICSKTART_TEST,
            upload_method="unencrypted",
        )

    elif PII_Flag.lower() == "true":
        console_printer.print_info(
            f"PII_flg set {PII_Flag}. Hence while will be upload directly from s3 bucket with record level encryption"
        )
        kickstart_adg_helper.files_upload_to_s3(
            context,
            list_of_local_files,
            folder_name=S3_KEY_KICSKTART_TEST,
            upload_method="encrypted",
        )


@when("Start kickstart adg emr process for modules '{modules}' and get step ids")
def step_impl(context, modules):
    emr_launcher_config = {}
    additional_step_args = {}
    KICKSTART_MODULES = modules.split(",")
    for module_name in KICKSTART_MODULES:
        correlation_id = f"kickstart_{module_name}_analytical_dataset_generation"
        data_product_name = "KICKSTART-ADG"
        processing_dt = datetime.strftime(
            datetime.now() - timedelta(days=1), "%Y-%m-%d"
        )
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

        additional_step_args.update(
            {
                f"submit-job-{module_name}": [
                    "--module_name",
                    f"{module_name}",
                    "--e2e_test_flg",
                    "True",
                ]
            }
        )

    emr_launcher_config.update({"additional_step_args": additional_step_args})

    console_printer.print_info(
        f"Launching the kickstart adg emr cluster for {module_name}"
    )
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
    context.kickstart_step_ids = []
    for step_name in additional_step_args:
        step = aws_helper.get_emr_cluster_step(step_name, cluster_id)
        step_id = step["Id"]
        context.kickstart_step_ids.append(step_id)
        console_printer.print_info(f"Step id for '{step_name}' : '{step_id}'")


@when(
    "Add validation steps '{step_name}' to kickstart adg emr cluster for '{module_name}' and add step Ids to the list"
)
def step_impl(context, step_name, module_name):

    context.kickstart_adg_hive_cluster_step_name = f"{module_name}-{step_name}"
    context.kickstart_hive_result_path = f"{S3_KEY_KICSKTART_TEST}"
    schema_config = context.kickstart_schema_config[module_name]
    console_printer.print_info(f"generating the list of hive queries to be executed")

    hive_queries_list = kickstart_adg_helper.generate_hive_queries(
        schema_config, context.published_bucket, context.kickstart_hive_result_path
    )

    console_printer.print_info(
        f"add hive queries as step to kickstart adg EMR cluster to get end result"
    )
    for hive_query in hive_queries_list:
        kickstart_hive_query_step_id = emr_step_generator.generate_bash_step(
            context.kickstart_adg_cluster_id,
            hive_query,
            context.kickstart_adg_hive_cluster_step_name,
        )
        context.kickstart_step_ids.append(kickstart_hive_query_step_id)


@then("Wait for all the steps to complete")
def step_impl(context):
    for step in context.kickstart_step_ids:
        console_printer.print_info(f"check if the step with {step} is complete or not")
        execution_state = aws_helper.poll_emr_cluster_step_status(
            step, context.kickstart_adg_cluster_id, 1200
        )
        if execution_state != COMPLETED_STATUS:
            raise AssertionError(
                f"The step Id {step} failed with final status of '{execution_state}'"
            )


@then("The input result matches with final output for module '{module_name}'")
def step_impl(context, module_name):
    schema_config = context.kickstart_schema_config[module_name]

    if schema_config["record_layout"].lower() == "csv":
        for collection in schema_config["schema"].keys():
            s3_result_key = os.path.join(
                context.kickstart_hive_result_path, f"e2e_{collection}.csv"
            )
            console_printer.print_info(f"S3 Request Location: {s3_result_key}")
            file_content = aws_helper.get_s3_object(
                None, context.published_bucket, s3_result_key
            ).decode("utf-8")
            actual_content = (
                file_content.replace("\t", ",")
                .replace("NULL", "None")
                .strip()
                .splitlines()
            )
            expected_file_name = [
                file
                for file in context.kickstart_current_run_input_files
                if collection in file
            ][0]
            console_printer.print_info(f"Expected File Name: {expected_file_name}")
            expected_content = file_helper.get_contents_of_file(
                expected_file_name, False
            ).splitlines()[1:]

            for input_line, output_line in zip(actual_content, expected_content):
                assert (
                    input_line.lower() == output_line.lower()
                ), f"Expected result of '{input_line}', does not match '{output_line}' for collection {collection}"

    elif schema_config["record_layout"].lower() == "json":
        for collection in schema_config["schema"].keys():
            s3_result_key = os.path.join(
                context.kickstart_hive_result_path, f"e2e_{collection}.csv"
            )
            console_printer.print_info(f"S3 Request Location: {s3_result_key}")
            file_content = aws_helper.get_s3_object(
                None, context.published_bucket, s3_result_key
            ).decode("utf-8")
            actual_content = file_content.replace("NULL", "None").strip().splitlines()
            console_printer.print_info(
                f"This the local file name in the list: {context.kickstart_current_run_input_files}"
            )
            expected_file_name = [
                file
                for file in context.kickstart_current_run_input_files
                if f"{module_name}-{collection}" in file
            ][0]
            console_printer.print_info(f"Expected File Name: {expected_file_name}")
            expected_json = json.loads(
                file_helper.get_contents_of_file(expected_file_name, False)
            )["data"]
            expected_content = "\n".join(
                [
                    "\t".join([str(record[field]) for field in record])
                    for record in expected_json
                ]
            ).splitlines()

            for input_line, output_line in zip(actual_content, expected_content):
                assert (
                    input_line.lower() == output_line.lower()
                ), f"Expected result of '{input_line}', does not match '{output_line}' for collection {collection}"
