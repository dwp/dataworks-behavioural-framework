import json

from behave import given, when, then
from helpers import aws_helper, dataworks_kafka_consumer_helper, console_printer, emr_step_generator


@given("The checksums are uploaded")
def step_impl(context):
    # TODO: Specify S3 Bucket used for exports and upload checksums
    console_printer.print_warning_text(context.db_object_checksums)

@when("The HBASE Snapshot Export Flow is run")
def step_impl(context):
    # TODO: Specify script to run that will do the export
    bash_script = "echo 'hello world'"
    step_type = "HBASE Export"
    context.ingest_hbase_emr_job_step_id = emr_step_generator.generate_bash_step(
        context.ingest_hbase_emr_cluster_id,
        bash_script,
        step_type,
    )

@when("The HBASE Snapshot Export Flow is executed successfully")
def step_impl(context):
    step_type = "HBASE Export"

    execution_state = aws_helper.poll_emr_cluster_step_status(
        context.ingest_hbase_emr_job_step_id, context.ingest_hbase_emr_cluster_id
    )

    if execution_state != "COMPLETED":
        raise AssertionError(
            f"'{step_type}' step failed with final status of '{execution_state}'"
        )

@then("The Snapshot is available in the S3 bucket")
def step_impl(context):
    # TODO: Specify S3 Bucket used for exports and check for snapshot and checksums 
    pass
