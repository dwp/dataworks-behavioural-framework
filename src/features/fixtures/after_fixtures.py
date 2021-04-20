import os
from behave import fixture
from helpers import (
    aws_helper,
    console_printer,
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
            console_printer.print_warning(
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
            console_printer.print_warning(
                f"Error occured when terminating clive cluster with id of '{context.clive_cluster_id}' as the following error occurred: '{error}'"
            )

    else:
        console_printer.print_info(
            "No cluster id found for clive so not terminating any cluster"
        )


@fixture
def terminate_pdm_cluster(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'terminate_pdm_cluster' fixture")

    if "pdm_cluster_id" in context and context.pdm_cluster_id is not None:
        try:
            aws_helper.terminate_emr_cluster(context.pdm_cluster_id)
        except ClientError as error:
            console_printer.print_warning(
                f"Error occured when terminating PDM cluster with id of '{context.adg_cluster_id}' as the following error occurred: '{error}'"
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
            console_printer.print_warning(
                f"Error occured when terminating kickstart cluster with id of '{context.adg_cluster_id}' as the following error occurred: '{error}'"
            )
    else:
        console_printer.print_info(
            f"No cluster id found for PDM so not terminating any cluster"
        )
