import os
from behave import when, then, given
from helpers import aws_helper, emr_step_generator


@when("An emrfs '{step_type}' step is started on the ingest-hbase EMR cluster")
def step_impl(context, step_type):
    s3_prefix = (
        context.ingest_hbase_emrfs_prefix_override
        if context.ingest_hbase_emrfs_prefix_override
        else context.ingest_hbase_emr_cluster_root_s3_root_directory
    )

    context.ingest_hbase_emr_job_step_id = emr_step_generator.generate_emrfs_step(
        context.ingest_hbase_emr_cluster_id,
        context.ingest_hbase_emr_cluster_root_s3_bucket_id,
        s3_prefix,
        step_type,
        context.ingest_hbase_emrfs_arguments,
    )


@when("A script '{step_type}' step is started on the ingest-hbase EMR cluster")
def step_impl(context, step_type):
    script_name = None
    arguments = None

    if step_type == "major compaction":
        script_name = "/var/ci/major_compaction_script.sh"
    elif step_type == "download scripts":
        script_name = "/var/ci/download_scripts.sh"
    elif step_type == "generate snapshots":
        script_name = "/var/ci/snapshot_tables_script.sh"
        arguments = context.ingest_hbase_snapshot_tables_override
    elif step_type == "hbck":
        script_name = "/var/ci/hbck_details_script.sh"
        arguments = context.ingest_hbase_hbck_arguments

    if script_name:
        context.ingest_hbase_emr_job_step_id = emr_step_generator.generate_script_step(
            context.ingest_hbase_emr_cluster_id,
            script_name,
            step_type,
            arguments,
        )


@given("A bash '{step_type}' step is started on the ingest-hbase EMR cluster")
@when("A bash '{step_type}' step is started on the ingest-hbase EMR cluster")
def step_impl(context, step_type):
    bash_script = None

    if step_type == "drop all tables":
        bash_script = "echo -e \"drop_all '.*'\\ny\" | hbase shell"
    elif step_type == "disable all tables":
        bash_script = "hbase shell <<< list | egrep '^[a-z]' | grep -v '^list' | while read; do echo -e \"disable '$REPLY'\"; done | hbase shell"
    elif step_type == "download cdl script":
        bash_script = f"aws s3 cp {context.cdl_run_script_s3_url} /opt/emr/run_cdl.sh && chmod +x /opt/emr/run_cdl.sh"
    elif step_type == "download cdl input split script":
        bash_script = f"aws s3 cp {context.cdl_split_inputs_s3_url} /opt/emr/split_inputs.pl && chmod +x /opt/emr/split_inputs.pl"
    elif step_type == "download hdl script":
        bash_script = f"aws s3 cp {context.hdl_run_script_s3_url} /opt/emr/run_hdl.sh && chmod +x /opt/emr/run_hdl.sh"
    elif step_type == "download create tables script":
        bash_script = f"aws s3 cp {context.create_hbase_tables_script_url} /opt/emr/create_hbase_tables.sh && chmod +x /opt/emr/create_hbase_tables.sh"
    elif step_type == "disable cleaner chore":
        bash_script = (
            "echo $'cleaner_chore_enabled; cleaner_chore_switch false' | hbase shell"
        )
    elif step_type == "enable cleaner chore":
        bash_script = (
            "echo $'cleaner_chore_enabled; cleaner_chore_switch true' | hbase shell"
        )
    elif step_type == "disable balancer":
        bash_script = "echo $'balance_switch false' | hbase shell"
    elif step_type == "enable balancer":
        bash_script = "echo $'balance_switch true' | hbase shell"

    if bash_script:
        context.ingest_hbase_emr_job_step_id = emr_step_generator.generate_bash_step(
            context.ingest_hbase_emr_cluster_id,
            bash_script,
            step_type,
        )


@given("The '{step_type}' step is executed successfully")
@when("The '{step_type}' step is executed successfully")
def step_impl(context, step_type):
    execution_state = aws_helper.poll_emr_cluster_step_status(
        context.ingest_hbase_emr_job_step_id, context.ingest_hbase_emr_cluster_id
    )

    if execution_state != "COMPLETED":
        raise AssertionError(
            f"'{step_type}' step failed with final status of '{execution_state}'"
        )
