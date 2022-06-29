import ast
from behave import given, when, then
import os
import json

from helpers import (
    ch_helper,
    aws_helper,
    invoke_lambda,
    emr_step_generator,
    historic_data_load_generator,
    console_printer,
    file_helper,
    kickstart_adg_helper,
)

CONF_FILENAME = "conf.tpl"
CONF_PREFIX = "component/kickstart-analytical-dataset-generation/steps/utils/"
E2E_S3_PREFIX = "e2e-ch/companies"

@when("The cluster starts without steps")
def step_impl(context):
    emr_launcher_config = {
        "s3_overrides": None,
        "overrides": {
            "Name": "ch-e2e",
            "Instances": {"KeepJobFlowAliveWhenNoSteps": True},
            "Steps": [],
        },
        "extend": None,
        "additional_step_args": None,
    }
    payload_json = json.dumps(emr_launcher_config)
    cluster_response = invoke_lambda.invoke_kickstart_adg_emr_launcher_lambda(
        payload_json
    )
    cluster_arn = cluster_response["ClusterArn"]
    cluster_arn_arr = cluster_arn.split(":")
    cluster_identifier = cluster_arn_arr[len(cluster_arn_arr) - 1]
    cluster_identifier_arr = cluster_identifier.split("/")
    cluster_id = cluster_identifier_arr[len(cluster_identifier_arr) - 1]
    context.ch_cluster_id = cluster_id
    print(f"found cluster iud {context.ch_cluster_id}")
    console_printer.print_info(f"Started emr cluster : '{cluster_id}'")

@then("Download the file that includes the etl arguments from s3 and parse it")
def step_impl(context):
    if not os.path.isdir(context.temp_folder):
        os.mkdir(context.temp_folder)
    ch_helper.download_file(context.common_config_bucket, CONF_PREFIX, CONF_FILENAME, context.temp_folder)
    args = ch_helper.get_args(os.path.join(context.temp_folder, CONF_FILENAME))
    context.args_ch = args

@then("Generate '{nfiles_per_date}' files with '{nrecords}' rows for both todays and yesterdays date")
def step_impl(context, nfiles_per_date, nrecords):
    console_printer.print_info(f"generating files fro the column {context.args_ch['args']['cols']}")

    context.filenames = ch_helper.get_filenames(context.args_ch['args']['filename'],
                                                int(nfiles_per_date), context.temp_folder)
    console_printer.print_info(f"filenames are {context.filenames}")
    cols = ast.literal_eval(context.args_ch['args']['cols'])
    ch_helper.generate_csv_files(context.filenames, nrecords, cols)
    ch_helper.generate_csv_files(context.filenames, nrecords, cols.append("newcol"))
    context.rows_expected = int(nfiles_per_date) * int(nrecords) * 2 - int(nrecords)  # subtract latest processed files records
    context.cols_expected = len(cols)+1  # pre-defined columns plus partitioning column


@then("Upload the local files to s3")
def step_impl(context):
    console_printer.print_info(f"generated files with columns {context.args_ch['args']['cols']}")
    for f in context.filenames:
        ch_helper.upload_onto_s3(context, f, E2E_S3_PREFIX)
    context.filename_not_to_process = context.filenames[0]
    context.filenames_expected = context.filenames[1:]

@then("Set the dynamo db bookmark on the first filename generated")
def step_impl(context):
    ch_helper.filename_latest_dynamo_add(context)

@then("Add the company etl step in e2e mode and wait for it to complete")
def step_impl(context):

    command = " ".join(
        [
            "spark-submit /opt/emr/steps/utils/company_etl.py",
            "--e2e True"
        ]
    )
    step_name = "spark-submit-company"
    step = emr_step_generator.generate_bash_step(
        emr_cluster_id=context.ch_cluster_id, bash_command=command, step_type=step_name
    )
    execution_state = aws_helper.poll_emr_cluster_step_status(
        step, context.ch_cluster_id, 2000
    )
    if execution_state != "COMPLETED":
        raise AssertionError(
            f"'{step_name}' step failed with final status of '{execution_state}'"
        )

@then("Add validation step and verify it completes")
def step_impl(context):

    command = " ".join(
        [
            "python3 /opt/emr/steps/e2e.py",
            f"--rows {context.rows_expected}",
            f"--cols {context.cols_expected}",
            f"--db {context.args_ch['args']['db_name']}",
            f"--table {context.args_ch['args']['table_name']}"
        ]
    )
    step_name = "e2e"
    step = emr_step_generator.generate_bash_step(
        emr_cluster_id=context.ch_cluster_id, bash_command=command, step_type=step_name
    )
    execution_state = aws_helper.poll_emr_cluster_step_status(
        step, context.ch_cluster_id, 2000
    )
    if execution_state != "COMPLETED":
        raise AssertionError(
            f"'{step_name}' step failed with final status of '{execution_state}'"
        )

@then("Verify last imported file was updated on DynamoDB")
def step_impl(context):
    assert ch_helper.file_latest_dynamo_fetch in context.filenames[-1], "the dynamoDB item was not updated correctly"



