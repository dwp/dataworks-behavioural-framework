from behave import given, when, then
import os
import json
import gzip
from datetime import datetime, timedelta
from helpers import (
    emr_step_generator,
    aws_helper,
    invoke_lambda,
    console_printer,
    file_helper,
    kickstart_adg_helper,
    historic_data_load_generator,
)

S3_KEY_KICSKTART_TEST = "kickstart-e2e-tests"
COMPLETED_STATUS = "COMPLETED"
CLUSTER_ARN = "ClusterArn"
AUDIT_TABLE_HASH_KEY = "Correlation_Id"
AUDIT_TABLE_RANGE_KEY = "DataProduct"
DYNAMO_DB_TABLE_NAME = "data_pipeline_metadata"


@given(
    "The template file '{template_name}' as an input, generate '{record_count}' records per table for '{module_name}'"
)
def step_impl(context, template_name, record_count, module_name):

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


@then("upload the local files to s3 bucket in '{data_encryption}' format")
def step_impl(context, data_encryption):

    kickstart_adg_helper.files_upload_to_s3(
        context,
        local_file_list=context.kickstart_current_run_input_files,
        folder_name=S3_KEY_KICSKTART_TEST,
        upload_method=data_encryption,
    )
