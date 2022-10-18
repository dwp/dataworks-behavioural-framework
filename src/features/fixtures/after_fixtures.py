import os
from behave import fixture
from botocore.exceptions import ClientError
from helpers import (
    aws_helper,
    console_printer,
    dataworks_kafka_producer_common_helper,
    emr_step_generator,
    data_ingress_helper,
)


@fixture
def clean_up_role_and_s3_objects(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'clean_up_role_and_s3_objects' fixture")

    aws_helper.remove_role(
        context.analytical_test_e2e_role, context.analytical_test_e2e_policies
    )

    aws_helper.clear_session()
    aws_helper.set_details_for_role_assumption(
        context.aws_role, context.aws_session_timeout_seconds
    )

    if context.analytical_test_data_s3_location.get("path"):
        aws_helper.remove_file_from_s3_and_wait_for_consistency(
            context.published_bucket,
            os.path.join(
                context.analytical_test_data_s3_location["path"],
                context.analytical_test_data_s3_location["file_name"],
            ),
        )

    if context.analytical_test_data_s3_location.get("paths"):
        for path in context.analytical_test_data_s3_location["paths"]:
            aws_helper.remove_file_from_s3_and_wait_for_consistency(
                context.published_bucket,
                os.path.join(
                    path, context.analytical_test_data_s3_location["file_name"]
                ),
            )


@fixture
def clean_up_s3_object(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'clean_up_s3_object' fixture")

    if aws_helper.check_if_s3_object_exists(
        context.published_bucket, context.analytical_test_data_s3_location.get("path")
    ):
        aws_helper.remove_file_from_s3_and_wait_for_consistency(
            context.published_bucket,
            os.path.join(
                context.analytical_test_data_s3_location["path"],
                context.analytical_test_data_s3_location["file_name"],
            ),
        )


@fixture
def terminate_adg_cluster(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'terminate_adg_cluster' fixture")

    if "adg_cluster_id" in context and context.adg_cluster_id is not None:
        try:
            aws_helper.terminate_emr_cluster(context.adg_cluster_id)
        except ClientError as error:
            console_printer.print_warning_text(
                f"Error occured when terminating ADG cluster with id of '{context.adg_cluster_id}' as the following error occurred: '{error}'"
            )

    else:
        console_printer.print_info(
            "No cluster id found for ADG so not terminating any cluster"
        )


@fixture
def terminate_clive_cluster(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'terminate_clive_cluster' fixture")

    if "clive_cluster_id" in context and context.clive_cluster_id is not None:
        try:
            aws_helper.terminate_emr_cluster(context.clive_cluster_id)
        except ClientError as error:
            console_printer.print_warning_text(
                f"Error occured when terminating clive cluster with id of '{context.clive_cluster_id}' as the following error occurred: '{error}'"
            )

    else:
        console_printer.print_info(
            "No cluster id found for clive so not terminating any cluster"
        )


@fixture
def terminate_uc_feature_cluster(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'terminate_uc_feature_cluster' fixture")

    if "uc_feature_cluster_id" in context and context.uc_feature_cluster_id is not None:
        try:
            aws_helper.terminate_emr_cluster(context.uc_feature_cluster_id)
        except ClientError as error:
            console_printer.print_warning_text(
                f"Error occured when terminating uc feature cluster with id of '{context.uc_feature_cluster_id}' as the following error occurred: '{error}'"
            )

    else:
        console_printer.print_info(
            "No cluster id found for uc feature so not terminating any cluster"
        )


@fixture
def terminate_pdm_cluster(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'terminate_pdm_cluster' fixture")

    if "pdm_cluster_id" in context and context.pdm_cluster_id is not None:
        try:
            aws_helper.terminate_emr_cluster(context.pdm_cluster_id)
        except ClientError as error:
            console_printer.print_warning_text(
                f"Error occured when terminating PDM cluster with id of '{context.pdm_cluster_id}' as the following error occurred: '{error}'"
            )
    else:
        console_printer.print_info(
            "No cluster id found for PDM so not terminating any cluster"
        )


@fixture
def terminate_kickstart_cluster(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'terminate_kickstart_adg_cluster' fixture")

    if (
        "kickstart_adg_cluster_id" in context
        and context.kickstart_adg_cluster_id is not None
    ):
        try:
            aws_helper.terminate_emr_cluster(context.kickstart_adg_cluster_id)
        except ClientError as error:
            console_printer.print_warning_text(
                f"Error occured when terminating kickstart cluster with id of '{context.kickstart_adg_cluster_id}' as the following error occurred: '{error}'"
            )
    else:
        console_printer.print_info(
            f"No cluster id found for PDM so not terminating any cluster"
        )


@fixture
def terminate_mongo_latest_cluster(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'terminate_mongo_latest_cluster' fixture")

    if (
        "mongo_latest_cluster_id" in context
        and context.mongo_latest_cluster_id is not None
    ):
        try:
            aws_helper.terminate_emr_cluster(context.mongo_latest_cluster_id)
        except ClientError as error:
            console_printer.print_warning_text(
                f"Error occured when terminating mongo latest cluster with id of '{context.mongo_latest_cluster_id}' as the following error occurred: '{error}'"
            )

    else:
        console_printer.print_info(
            "No cluster id found for mongo latest so not terminating any cluster"
        )


@fixture
def terminate_ingest_replica_cluster(context):
    console_printer.print_info("Executing 'terminate_ingest_replica_cluster' fixture")
    if (
        "ingest_replica_emr_cluster_id" in context
        and context.ingest_replica_emr_cluster_id is not None
    ):
        try:
            aws_helper.terminate_emr_cluster(context.ingest_replica_emr_cluster_id)
        except Exception as e:
            console_printer.print_warning_text(
                f"Unable to terminate cluster due to error:{e}"
            )
    else:
        console_printer.print_warning_text(
            "No ingest-replica cluster identified to terminate"
        )


@fixture
def terminate_cyi_cluster(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'terminate_cyi_cluster' fixture")

    if "cyi_cluster_id" in context and context.cyi_cluster_id is not None:
        try:
            aws_helper.terminate_emr_cluster(context.cyi_cluster_id)
        except ClientError as error:
            console_printer.print_warning_text(
                f"Error occured when terminating CYI cluster with id of '{context.cyi_cluster_id}' as the following error occurred: '{error}'"
            )
    else:
        console_printer.print_info(
            "No cluster id found for CYI so not terminating any cluster"
        )


@fixture
def terminate_datsci_cluster(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'terminate_datsci_cluster' fixture")

    if "datsci_cluster_id" in context and context.datsci_cluster_id is not None:
        try:
            aws_helper.terminate_emr_cluster(context.datsci_cluster_id)
        except ClientError as error:
            console_printer.print_warning_text(
                f"Error occured when terminating datsci cluster with id of '{context.datsci_cluster_id}' as the following error occurred: '{error}'"
            )

    else:
        console_printer.print_info(
            "No cluster id found for datasci so not terminating any cluster"
        )


@fixture
def dataworks_stop_kafka_producer_app(context):
    dataworks_kafka_producer_common_helper.dataworks_stop_kafka_producer_app(context)


@fixture
def dataworks_stop_kafka_consumer_app(context):
    console_printer.print_info("Executing 'stop_kafka_consumer_app' fixture")

    # Get instance id
    instance_id = aws_helper.get_instance_id("dataworks-kafka-consumer")

    # Execute the shell script - stop the e2e test application
    console_printer.print_info("Stopping e2e test application")
    linux_command = "sh /home/ec2-user/kafka/utils/stop_e2e_tests.sh"
    aws_helper.execute_linux_command(
        instance_id=instance_id,
        linux_command=linux_command,
    )

    # Clear S3 bucket
    console_printer.print_info(
        f"Stopping e2e tests...remove any data from s3 bucket: {context.dataworks_kafka_dlq_output_bucket}, prefix: {context.dataworks_dlq_output_s3_prefix}"
    )
    aws_helper.clear_s3_prefix(
        s3_bucket=context.dataworks_kafka_dlq_output_bucket,
        path=context.dataworks_dlq_output_s3_prefix,
        delete_prefix=True,
    )


@fixture
def clean_up_hbase_export_hbase_snapshots(context):
    bash_script = (
        f"echo \"delete_snapshot '{context.hbase_snapshot_name}'\" | hbase shell -n"
    )
    step_type = "Cleanup HBASE Snapshot"
    context.ingest_hbase_emr_job_step_id = emr_step_generator.generate_bash_step(
        context.ingest_hbase_emr_cluster_id,
        bash_script,
        step_type,
    )


@fixture
def stop_data_ingress(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'stop_data_ingress' fixture")

    try:
        data_ingress_helper.set_asg_instance_count("data-ingress-ag", 0, 0, 0)
    except Exception as error:
        console_printer.print_warning_text(
            f"Error occured when shutting down instances in data-ingress-ag as the following error occurred: '{error}'"
        )
