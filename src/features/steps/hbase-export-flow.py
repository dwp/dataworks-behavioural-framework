import json

from behave import given, when, then
from helpers import aws_helper, dataworks_kafka_consumer_helper, console_printer, emr_step_generator


@given("The checksums are uploaded")
def step_impl(context):
    # TODO: Specify S3 Bucket used for exports and upload checksums
    console_printer.print_warning_text(context.db_object_checksums)
    context.hbase_export_bucket
    for checksum in context.db_object_checksums:
        aws_helper.put_object_in_s3_with_metadata(
            body=b'',
            s3_bucket=context.hbase_export_bucket,
            s3_key="{}.md5".format(checksum),
            metadata={}
        )

@when("The HBASE Snapshot Export script is downloaded on the ingest-hbase EMR cluster")
def step_impl(context):
    # TODO: Specify script to run that will do the export
    bash_script = f"aws s3 cp {context.hbase_snapshot_exporter_script} /opt/emr/hbase-snapshot-exporter.sh && chmod +x /opt/emr/hbase-snapshot-exporter.sh"
    step_type = "Download HBASE Export script"
    context.ingest_hbase_emr_job_step_id = emr_step_generator.generate_bash_step(
        context.ingest_hbase_emr_cluster_id,
        bash_script,
        step_type,
    )

@when("The HBASE Snapshot Export script is run")
def step_impl(context):
    script_name = "/opt/emr/hbase-snapshot-exporter.sh"
    arguments = "{} s3://{}/".format("automatedtests:rorybryett_hbase_export_1_1", context.hbase_export_bucket)
    step_type = "HBASE Snapshot Export"

    context.ingest_hbase_emr_job_step_id = emr_step_generator.generate_script_step(
        context.ingest_hbase_emr_cluster_id,
        script_name,
        step_type,
        arguments,
    )

@given("The Download HBASE Export script step is executed successfully")
@when("The Download HBASE Export script step is executed successfully")
def step_impl(context):
    step_checker(context, "Download HBASE Export script")
    
@given("And The HBASE Snapshot Export step is executed successfully")
@when("And The HBASE Snapshot Export step is executed successfully")
def step_impl(context):
    step_checker(context, "The HBASE Snapshot Export")
    
def step_checker(context, step_type):
    execution_state = aws_helper.poll_emr_cluster_step_status(
        context.ingest_hbase_emr_job_step_id, context.ingest_hbase_emr_cluster_id, timeout_in_seconds=180
    )

    if execution_state != "COMPLETED":
        raise AssertionError(
            f"'{step_type}' step failed with final status of '{execution_state}'"
        )

@then("The Snapshot is available in the S3 bucket")
def step_impl(context):
    # TODO: Specify S3 Bucket used for exports and check for snapshot and checksums 
    pass
