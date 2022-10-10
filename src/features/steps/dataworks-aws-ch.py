import ast
from behave import given, when, then
import os
import json
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
CLUSTER_ARN = "ClusterArn"
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
    cluster_arn = cluster_response[CLUSTER_ARN]
    cluster_arn_arr = cluster_arn.split(":")
    cluster_identifier = cluster_arn_arr[len(cluster_arn_arr) - 1]
    cluster_identifier_arr = cluster_identifier.split("/")
    cluster_id = cluster_identifier_arr[len(cluster_identifier_arr) - 1]
    context.ch_cluster_id = cluster_id
    console_printer.print_info(f"Started emr cluster : '{cluster_id}'")


@when("The cluster is still running")
def step_impl(context):
    if aws_helper.poll_emr_cluster_status(context.ch_cluster_id) not in (
        "WAITING",
        "TERMINATED",
        "TERMINATED_WITH_ERRORS",
    ):
        console_printer.print_info(
            f"Cluster {context.ch_cluster_id} ready for new steps"
        )


@then("Download and parse conf file")
def step_impl(context):
    if not os.path.isdir(context.temp_folder):
        os.mkdir(context.temp_folder)
    ch_helper.download_file(
        context.common_config_bucket, CONF_PREFIX, CONF_FILENAME, context.temp_folder
    )
    print(os.listdir(context.temp_folder))
    args = ch_helper.get_args(os.path.join(context.temp_folder, CONF_FILENAME))
    context.args_ch = args


@then("Generate files having expected format and size to test positive outcome")
def step_impl(context):
    console_printer.print_info(
        f"generating files from the columns {context.args_ch['args']['cols']}"
    )
    context.filenames = ch_helper.get_filenames(
        context.args_ch["args"]["filename"], context.temp_folder
    )
    console_printer.print_info(f"filenames are {context.filenames}")
    cols = ast.literal_eval(context.args_ch["args"]["cols"])
    console_printer.print_info(f"generating {context.filenames[0]} ")
    ch_helper.generate_csv_file(context.filenames[0], 0.09, cols)
    console_printer.print_info(f"generating {context.filenames[1]} ")
    ch_helper.generate_csv_file(context.filenames[1], 0.099, cols)
    file = open(context.filenames[1])
    reader = csv.reader(file)
    lines = len(list(reader))
    context.rows_expected = lines - 1  # do not count header
    context.cols_expected = (
        len(cols) + 1
    )  # pre-defined columns + the partitioning column


@then("Upload the local file to s3")
def step_impl(context):
    console_printer.print_info(
        f"generated files with columns {context.args_ch['args']['cols']}"
    )
    for f in context.filenames:
        ch_helper.s3_upload(context, f, E2E_S3_PREFIX)
    context.filename_not_to_process = context.filenames[0]
    context.filename_expected = context.filenames[-1]


@then("Set the dynamo db bookmark on the first filename generated")
def step_impl(context):
    ch_helper.add_latest_file(
        context, os.path.join(E2E_S3_PREFIX, os.path.basename(context.filenames[0]))
    )


@then("Add the etl step in e2e mode and wait for it to complete")
def step_impl(context):

    command = " ".join(
        [
            "spark-submit --master yarn --conf spark.yarn.submit.waitAppCompletion=true /opt/emr/etl.py",
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


@then("Add the etl step in e2e mode and wait for it to fail")
def step_impl(context):

    command = " ".join(
        [
            "spark-submit --master yarn --conf spark.yarn.submit.waitAppCompletion=true /opt/emr/etl.py",
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
    if execution_state != "FAILED":
        raise AssertionError(
            f"'{step_name}' step failed with final status of '{execution_state}'"
        )


@then("Add validation step and verify it completes")
def step_impl(context):
    command = " ".join(
        [
            "python3 /opt/emr/e2e.py",
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


@then("Generate files having wrong size for negative testing")
def step_impl(context):
    console_printer.print_info(
        f"generating files fro the column {context.args_ch['args']['cols']}"
    )
    console_printer.print_info(f"filenames are {context.filenames}")
    cols = ast.literal_eval(context.args_ch["args"]["cols"])
    ch_helper.generate_csv_file(context.filenames[0], 0.001, cols)
    ch_helper.generate_csv_file(context.filenames[1], 0.04, cols)


@then("Verify last imported file was updated on DynamoDB")
def step_impl(context):

    filename_expected = os.path.join(
        E2E_S3_PREFIX, os.path.basename(context.filenames[1])
    )

    filename_from_table = ch_helper.get_latest_file(context)
    assert (
        filename_from_table == filename_expected
    ), "the dynamoDB item was not updated correctly"


@then("Verify that the alarms turned on due to wrong file size")
def step_impl(context):
    start = time.time()
    while not ch_helper.did_alarm_trigger("file_size_check_failed"):
        if time.time() - start < TIMEOUT:
            time.sleep(5)
        else:
            raise AssertionError(f"alarm did not trigger after {TIMEOUT} seconds")


@then("Verify that the alarms turned on due to wrong file format")
def step_impl(context):
    start = time.time()
    while not ch_helper.did_alarm_trigger("file_format_check_failed"):
        if time.time() - start < TIMEOUT:
            time.sleep(5)
        else:
            raise AssertionError(f"alarm did not trigger after {TIMEOUT} seconds")


@then("Clear S3 prefix where previous synthetic data is")
def step_impl(context):
    console_printer.print_info("clearing source prefix")
    aws_helper.clear_s3_prefix(context.data_ingress_stage_bucket, E2E_S3_PREFIX, False)


@then("Generate files having one extra column for negative testing")
def step_impl(context):
    console_printer.print_info(f"generating files with one extra column")
    cols = ast.literal_eval(context.args_ch["args"]["cols"])
    cols=cols.update({"extra_column":"string"})
    ch_helper.generate_csv_file(context.filenames[0], 0.09, cols)
    ch_helper.generate_csv_file(context.filenames[1], 0.099, cols)
    start = time.time()
    while not ch_helper.did_alarm_trigger("file_format_check_failed"):
        if time.time() - start < TIMEOUT:
            time.sleep(5)
        else:
            raise AssertionError(f"alarm did not trigger after {TIMEOUT} seconds")


@then("Generate files having one column less for negative testing")
def step_impl(context):
    console_printer.print_info(f"generating files with one column less")
    cols = ast.literal_eval(context.args_ch["args"]["cols"])
    cols.pop(cols.keys()[-1])
    ch_helper.generate_csv_file(context.filenames[0], 0.09, cols)
    ch_helper.generate_csv_file(context.filenames[1], 0.099, cols)
    start = time.time()
    while not ch_helper.did_alarm_trigger("file_format_check_failed"):
        if time.time() - start < TIMEOUT:
            time.sleep(5)
        else:
            raise AssertionError(f"alarm did not trigger after {TIMEOUT} seconds")


@then("Generate files having incorrect headers for negative testing")
def step_impl(context):
    console_printer.print_info(f"generating files with wrong headers")
    cols = ast.literal_eval(context.args_ch["args"]["cols"])
    cols.pop(cols.keys()[-1])
    cols.pop(cols.keys()[-1])
    cols = cols.update({"incorrect_colname_1":"string","incorrect_colname_2":"string"})
    ch_helper.generate_csv_file(context.filenames[0], 0.09, cols)
    ch_helper.generate_csv_file(context.filenames[1], 0.099, cols)
    start = time.time()
    while not ch_helper.did_alarm_trigger("file_format_check_failed"):
        if time.time() - start < TIMEOUT:
            time.sleep(5)
        else:
            raise AssertionError(f"alarm did not trigger after {TIMEOUT} seconds")


@then("Generate files having a row with string values instead of int")
def step_impl(context):
    console_printer.print_info(f"generating files with one missing field for negative testing")
    cols = ast.literal_eval(context.args_ch["args"]["cols"])
    ch_helper.generate_csv_file_string_instead_of_int(context.filenames[0], 0.09, cols)
    ch_helper.generate_csv_file_string_instead_of_int(context.filenames[1], 0.099, cols)
    start = time.time()
    while not ch_helper.did_alarm_trigger("file_format_check_failed"):
        if time.time() - start < TIMEOUT:
            time.sleep(5)
        else:
            raise AssertionError(f"alarm did not trigger after {TIMEOUT} seconds")


@then("Generate files having a row with one missing field for negative testing")
def step_impl(context):
    console_printer.print_info(f"generating files with one missing field for negative testing")
    cols = ast.literal_eval(context.args_ch["args"]["cols"])
    ch_helper.generate_csv_file_row_with_missing_field(context.filenames[0], 0.09, cols)
    ch_helper.generate_csv_file_row_with_missing_field(context.filenames[1], 0.099, cols)
    start = time.time()
    while not ch_helper.did_alarm_trigger("file_format_check_failed"):
        if time.time() - start < TIMEOUT:
            time.sleep(5)
        else:
            raise AssertionError(f"alarm did not trigger after {TIMEOUT} seconds")


