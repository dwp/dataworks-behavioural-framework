import json

from behave import given, when, then
from helpers import (
    aws_helper,
    dataworks_kafka_consumer_helper,
    console_printer,
    emr_step_generator,
    template_helper,
)


@given("The checksums are uploaded")
def step_impl(context):
    for checksum in context.db_object_checksums:
        aws_helper.put_object_in_s3_with_metadata(
            body=b"",
            s3_bucket=context.hbase_export_bucket,
            s3_key=f"snapshots/{checksum}.md5",
            metadata={}
        )


@when("The HBASE Snapshot Export script is downloaded on the ingest-hbase EMR cluster")
def step_impl(context):
    bash_script = f"aws s3 cp {context.hbase_snapshot_exporter_script} /opt/emr/hbase-snapshot-exporter.sh && chmod +x /opt/emr/hbase-snapshot-exporter.sh"
    step_type = "Download HBASE Export script"
    context.ingest_hbase_emr_job_step_id = emr_step_generator.generate_bash_step(
        context.ingest_hbase_emr_cluster_id,
        bash_script,
        step_type,
    )


@when(
    "The HBASE Snapshot Export script is run with HBASE snapshot name '{hbase_snapshot_name}'"
)
def step_impl(context, hbase_snapshot_name):
    script_name = "/opt/emr/hbase-snapshot-exporter.sh"
    context.hbase_snapshot_name = hbase_snapshot_name

    #TODO: support multiple topics / tables
    hbase_table = template_helper.get_hbase_table_name_fromt_topic_name(context.topics[0])
    arguments = f"{hbase_table} s3://{context.hbase_export_bucket}/snapshots {context.hbase_snapshot_name}"
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
    step_type = "Download HBASE Export script"
    step_checker(context, step_type)


@given("The HBASE Snapshot Export step is executed successfully")
@when("The HBASE Snapshot Export step is executed successfully")
def step_impl(context):
    step_type = "HBASE Snapshot Export"
    step_checker(context, step_type)


def step_checker(context, step_type):
    execution_state = aws_helper.poll_emr_cluster_step_status(
        context.ingest_hbase_emr_job_step_id,
        context.ingest_hbase_emr_cluster_id,
        timeout_in_seconds=300,
    )

    if execution_state != "COMPLETED":
        raise AssertionError(
            f"'{step_type}' step failed with final status of '{execution_state}'"
        )

@given("The HBASE Snapshot '{hbase_snapshot_name}' is available in the Export S3 bucket")
@when("The HBASE Snapshot '{hbase_snapshot_name}' is available in the Export S3 bucket")
@then("The HBASE Snapshot '{hbase_snapshot_name}' is available in the Export S3 bucket")
def step_impl(context, hbase_snapshot_name):
    if not aws_helper.does_s3_key_exist(context.hbase_export_bucket, f"snapshots/.hbase-snapshot/{hbase_snapshot_name}"):
        raise AssertionError(
            f"Snapshot was not exported to 's3://{context.hbase_export_bucket}/snapshots'"
        )

    if not aws_helper.does_s3_key_exist(context.hbase_export_bucket, f"snapshots/archive"):
        raise AssertionError(
            f"Snapshot was not exported to 's3://{context.hbase_export_bucket}/snapshots'"
        )
