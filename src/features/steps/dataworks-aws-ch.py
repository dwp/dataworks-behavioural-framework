import ast
from behave import given, when, then
import os
import json
import zipfile
import csv
import time
from helpers import (
    ch_helper,
    aws_helper,
    invoke_lambda,
    emr_step_generator,
    historic_data_load_generator,
    console_printer,
    file_helper,
)

CONF_FILENAME = "e2e_test_conf.tpl"
CONF_PREFIX = "component/dataworks-aws-ch/steps/"
E2E_S3_PREFIX = "e2e/data-ingress/companies"
TIMEOUT = 300


@when("The cluster starts without steps")
def step_impl(context):
    emr_launcher_config = {
        "s3_overrides": None,
        "overrides": {
            "Name": "dataworks-aws-ch-e2e",
            "Instances": {"KeepJobFlowAliveWhenNoSteps": True},
            "Steps": [],
        },
        "extend": None,
        "additional_step_args": None,
    }
    payload_json = json.dumps(emr_launcher_config)
    cluster_response = invoke_lambda.invoke_ch_emr_launcher_lambda(payload_json)
    console_printer.print_info(f"response : '{cluster_response}'")
    cluster_arn = cluster_response["ClusterArn"]
    cluster_arn_arr = cluster_arn.split(":")
    cluster_identifier = cluster_arn_arr[len(cluster_arn_arr) - 1]
    cluster_identifier_arr = cluster_identifier.split("/")
    cluster_id = cluster_identifier_arr[len(cluster_identifier_arr) - 1]
    context.ch_cluster_id = cluster_id
    console_printer.print_info(f"Started emr cluster : '{cluster_id}'")


@when("Download and parse conf file")
def step_impl(context):
    if not os.path.isdir(context.temp_folder):
        os.mkdir(context.temp_folder)
    ch_helper.download_file(
        context.common_config_bucket, CONF_PREFIX, CONF_FILENAME, context.temp_folder
    )
    print(os.listdir(context.temp_folder))
    args = ch_helper.get_args(os.path.join(context.temp_folder, CONF_FILENAME))
    context.args_ch = args


@when("Generate files having expected format and size to test positive outcome")
def step_impl(context):
    console_printer.print_info(
        f"generating files from the columns {context.args_ch['args']['cols']}"
    )
    context.filenames_zip_s3, context.filenames_csv_local, context.filenames_zip_local = ch_helper.get_filenames(
        context.args_ch["args"]["filename"], context.temp_folder
    )
    cols = ast.literal_eval(context.args_ch["args"]["cols"])
    console_printer.print_info(f"generating file 1 ")
    ch_helper.generate_csv_file(context.filenames_csv_local[0], 0.01, cols)
    console_printer.print_info(f"generating file 2")
    ch_helper.generate_csv_file(context.filenames_csv_local[1], 0.02, cols)
    file = open(context.filenames_csv_local[1])
    reader = csv.reader(file)
    lines = len(list(reader))
    context.rows_expected = lines - 1  # do not count header
    context.cols_expected = (
        len(cols) + 1
    )  # pre-defined columns + the partitioning column


@when("Zip and upload the local file to s3")
def step_impl(context):
    console_printer.print_info(
        f"generated files with columns {context.args_ch['args']['cols']}"
    )
    zip_f1 = zipfile.ZipFile(context.filenames_zip_local[0], "w", zipfile.ZIP_DEFLATED)
    zip_f1.write(context.filenames_csv_local[0])
    zip_f1.close()
    zip_f2 = zipfile.ZipFile(context.filenames_zip_local[1], "w", zipfile.ZIP_DEFLATED)
    zip_f2.write(context.filenames_csv_local[1])
    zip_f2.close()
    ch_helper.s3_upload(context, context.filenames_zip_local[0], E2E_S3_PREFIX, context.filenames_zip_s3[0])
    ch_helper.s3_upload(context, context.filenames_zip_local[1], E2E_S3_PREFIX, context.filenames_zip_s3[1])
    context.filename_not_to_process = context.filenames_zip_s3[0]
    context.filename_expected = context.filenames_zip_s3[-1]
    for i in [context.filenames_zip_local[0], context.filenames_zip_local[1], context.filenames_csv_local[0], context.filenames_csv_local[1]]:
        if os.path.exists(i):
            os.remove(i)


@when("Set the dynamo db bookmark on the first filename generated")
def step_impl(context):
    ch_helper.add_latest_file(
        context, os.path.basename(context.filenames_zip_s3[0]))


@then("Etl step in e2e mode completes")
def step_impl(context):

    command = " ".join(
        [
            "spark-submit --master yarn --conf spark.yarn.submit.waitAppCompletion=true /opt/emr/steps/etl.py",
            "--e2e True",
        ]
    )
    step_name = "etl"
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


@then("Etl step in e2e mode fails")
def step_impl(context):

    command = " ".join(
        [
            "spark-submit --master yarn --conf spark.yarn.submit.waitAppCompletion=true /opt/emr/steps/etl.py",
            "--e2e True",
        ]
    )
    step_name = "etl - negative testing"
    step = emr_step_generator.generate_bash_step(
        emr_cluster_id=context.ch_cluster_id, bash_command=command, step_type=step_name
    )
    execution_state = aws_helper.poll_emr_cluster_step_status(
        step, context.ch_cluster_id, 2000
    )
    if execution_state != "FAILED":
        raise AssertionError(
            f"'{step_name}' step failed with final status of '{execution_state}'"
        )


@then("Validation step completes")
def step_impl(context):
    command = " ".join(
        [
            "python3 /opt/emr/steps/e2e.py",
            f"--rows {context.rows_expected}",
            f"--cols {context.cols_expected}",
            f"--db {context.args_ch['args']['db_name']}",
            f"--table {context.args_ch['args']['table_name']}",
            f"--partitioning_column {context.args_ch['args']['partitioning_column']}",
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


@then("Last imported file is updated on DynamoDB")
def step_impl(context):

    filename_from_table = ch_helper.get_latest_file(context)
    assert (
        filename_from_table == context.filenames_zip_s3[1]
    ), "the dynamoDB item was not updated correctly"


@then("File format alarm triggers")
def step_impl(context):
    start = time.time()
    while not ch_helper.did_alarm_trigger("CH_file_format_check_failed"):
        if time.time() - start < TIMEOUT:
            time.sleep(5)
        else:
            raise AssertionError(f"alarm did not trigger after {TIMEOUT} seconds")


@when("Clear S3 prefix where previous synthetic data is")
def step_impl(context):
    console_printer.print_info("clearing source prefix")
    aws_helper.clear_s3_prefix(context.data_ingress_stage_bucket, E2E_S3_PREFIX, False)


@when("Generate files having one extra column for negative testing")
def step_impl(context):
    console_printer.print_info(f"generating files with one extra column")
    cols = ast.literal_eval(context.args_ch["args"]["cols"])
    cols.update({"extra_column": "string"})
    ch_helper.generate_csv_file(context.filenames_csv_local[0], 0.01, cols)
    ch_helper.generate_csv_file(context.filenames_csv_local[1], 0.02, cols)


@when("Generate files having incorrect headers for negative testing")
def step_impl(context):
    console_printer.print_info(f"generating files with wrong headers")
    cols = ast.literal_eval(context.args_ch["args"]["cols"])
    cols.pop(list(cols.keys())[-1])
    cols.pop(list(cols.keys())[-1])
    cols.update({"incorrect_colname_1": "string", "incorrect_colname_2": "string"})
    ch_helper.generate_csv_file(context.filenames_csv_local[0], 0.01, cols)
    ch_helper.generate_csv_file(context.filenames_csv_local[1], 0.02, cols)

