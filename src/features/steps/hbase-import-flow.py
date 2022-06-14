import json

from behave import given, when, then
from helpers import (
    aws_helper,
    dataworks_kafka_consumer_helper,
    console_printer,
    emr_step_generator,
    template_helper,
)


@given("The HBASE Snapshot Import script is downloaded on the ingest-hbase EMR cluster")
@when("The HBASE Snapshot Import script is downloaded on the ingest-hbase EMR cluster")
def step_impl(context):
    bash_script = f"aws s3 cp {context.hbase_snapshot_importer_script} /opt/emr/hbase-snapshot-importer.sh && chmod +x /opt/emr/hbase-snapshot-importer.sh"
    step_type = "Download HBASE Import script"
    context.ingest_hbase_emr_job_step_id = emr_step_generator.generate_bash_step(
        context.ingest_hbase_emr_cluster_id,
        bash_script,
        step_type,
    )


@then("The Download HBASE Import script step is executed successfully")
def step_impl(context):
    step_type = "Download HBASE Import script"
    step_checker(context, step_type)


@given(
    "The HBASE Snapshot '{hbase_snapshot_name}' is imported from the HBASE Export Bucket to the HBASE root dir"
)
@when(
    "The HBASE Snapshot '{hbase_snapshot_name}' is imported from the HBASE Export Bucket to the HBASE root dir"
)
def step_impl(context, hbase_snapshot_name):
    script_name = "/opt/emr/hbase-snapshot-importer.sh"
    context.hbase_snapshot_name = hbase_snapshot_name

    arguments = f"{context.hbase_snapshot_name} {context.hbase_export_bucket} snapshots"
    step_type = "HBASE Snapshot Import"

    context.ingest_hbase_emr_job_step_id = emr_step_generator.generate_script_step(
        context.ingest_hbase_emr_cluster_id,
        script_name,
        step_type,
        arguments,
    )

    step_checker(context, step_type)


@given(
    "The HBASE Snapshot Restore script is downloaded on the ingest-hbase EMR cluster"
)
@when("The HBASE Snapshot Restore script is downloaded on the ingest-hbase EMR cluster")
def step_impl(context):
    bash_script = f"aws s3 cp {context.hbase_snapshot_restorer_script} /opt/emr/hbase-snapshot-restorer.sh && chmod +x /opt/emr/hbase-snapshot-restorer.sh"
    step_type = "Download HBASE Restore script"
    context.ingest_hbase_emr_job_step_id = emr_step_generator.generate_bash_step(
        context.ingest_hbase_emr_cluster_id,
        bash_script,
        step_type,
    )


@then("The Download HBASE Restore script step is executed successfully")
def step_impl(context):
    step_type = "Download HBASE Restore script"
    step_checker(context, step_type)


@given("The imported HBASE Snapshot '{hbase_snapshot_name}' is restored")
@when("The imported HBASE Snapshot '{hbase_snapshot_name}' is restored")
@then("The imported HBASE Snapshot '{hbase_snapshot_name}' is restored")
def step_impl(context, hbase_snapshot_name):
    script_name = "/opt/emr/hbase-snapshot-restorer.sh"
    context.hbase_snapshot_name = hbase_snapshot_name

    arguments = f"restore {hbase_snapshot_name}"
    step_type = "HBASE Snapshot Restore"

    context.ingest_hbase_emr_job_step_id = emr_step_generator.generate_script_step(
        context.ingest_hbase_emr_cluster_id,
        script_name,
        step_type,
        arguments,
    )

    step_checker(context, step_type)


@given(
    "The imported HBASE Snapshot '{hbase_snapshot_name}' is cloned into a HBASE table"
)
@when(
    "The imported HBASE Snapshot '{hbase_snapshot_name}' is cloned into a HBASE table"
)
@then(
    "The imported HBASE Snapshot '{hbase_snapshot_name}' is cloned into a HBASE table"
)
def step_impl(context, hbase_snapshot_name):
    script_name = "/opt/emr/hbase-snapshot-restorer.sh"
    context.hbase_snapshot_name = hbase_snapshot_name

    arguments = f"clone {hbase_snapshot_name} {context.hbase_snapshot_cloned_table}"
    step_type = "HBASE Snapshot Clone"

    context.ingest_hbase_emr_job_step_id = emr_step_generator.generate_script_step(
        context.ingest_hbase_emr_cluster_id,
        script_name,
        step_type,
        arguments,
    )

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
