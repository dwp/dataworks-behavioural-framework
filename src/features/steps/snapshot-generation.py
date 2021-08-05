import json
from behave import given, when, then
from datetime import datetime
from helpers import (
    template_helper,
    aws_helper,
    file_helper,
    file_comparer,
    export_status_helper,
    historic_data_generator,
    console_printer,
    message_helper,
    kafka_data_generator,
    snapshots_helper,
    data_pipeline_metadata_helper,
    manifest_comparison_helper,
)


@given(
    "Snapshot sender is scaled if it will be triggered for snapshot type of '{snapshot_type}'"
)
def step_impl(context, snapshot_type):
    run_ss_after_export = (
        context.generate_snapshots_trigger_snapshot_sender_override == "true"
    )

    if run_ss_after_export:
        updated_topics = message_helper.get_consolidated_topics_list(
            context.topics,
            snapshot_type,
            context.default_topic_list_full_delimited,
            context.default_topic_list_incremental_delimited,
            context.default_topic_list_drift_testing_incrementals,
            [context.generate_snapshots_topics_override],
        )

        desired_count = (
            context.asg_max_count_snapshot_sender
            if not context.snapshot_sender_scale_up_override
            else context.snapshot_sender_scale_up_override
        )

        context.last_scaled_asg = (
            aws_helper.scale_asg_if_desired_count_is_not_already_set(
                context.asg_prefix_snapshot_sender, int(desired_count)
            )
        )


@when(
    "The export process is performed with default settings for snapshot type of '{snapshot_type}'"
)
def step_impl(context, snapshot_type):
    updated_topics = message_helper.get_consolidated_topics_list(
        None,
        snapshot_type,
        context.default_topic_list_full_delimited,
        context.default_topic_list_incremental_delimited,
        context.default_topic_list_drift_testing_incrementals,
        [context.generate_snapshots_topics_override],
    )
    start_time = (
        None
        if not context.generate_snapshots_start_time_override
        else context.generate_snapshots_start_time_override
    )
    end_time = (
        None
        if not context.generate_snapshots_end_time_override
        else context.generate_snapshots_end_time_override
    )

    topic_override = None if updated_topics is None else ",".join(updated_topics)

    correlation_id = (
        snapshots_helper.get_snapshot_run_correlation_id(
            context.test_run_name, snapshot_type
        )
        if not context.generate_snapshots_correlation_id_override
        else context.generate_snapshots_correlation_id_override
    )
    today_yyyymmdd = (
        context.generate_snapshots_export_date_override
        if context.generate_snapshots_export_date_override
        else datetime.now().strftime("%Y-%m-%d")
    )
    mongo_snapshot_full_s3_location = (
        f"{context.mongo_snapshot_path}/{today_yyyymmdd}/{snapshot_type}"
    )
    context.mongo_snapshot_full_s3_location = mongo_snapshot_full_s3_location

    message_helper.send_start_export_message(
        context.aws_sns_uc_ecc_arn,
        None,
        topic_override,
        start_time,
        end_time,
        context.test_run_name,
        context.generate_snapshots_trigger_snapshot_sender_override,
        context.generate_snapshots_reprocess_files,
        snapshot_type,
        correlation_id,
        context.export_process_trigger_adg_override,
        context.generate_snapshots_export_date_override,
        context.export_process_clear_s3_snapshots,
        context.export_process_clear_s3_manifests,
        mongo_snapshot_full_s3_location,
    )


@when(
    "The export and snapshot process is performed for snapshot type of '{snapshot_type}'"
)
def step_impl(context, snapshot_type):
    message_helper.send_start_export_message(
        context.aws_sns_uc_ecc_arn,
        context.test_run_name,
        context.topics_delimited,
        None,
        None,
        context.test_run_name,
        "true",
        context.generate_snapshots_reprocess_files,
        snapshot_type,
        snapshots_helper.get_snapshot_run_correlation_id(
            context.test_run_name, snapshot_type
        ),
        context.export_process_trigger_adg_override,
        context.generate_snapshots_export_date_override,
        context.export_process_clear_s3_snapshots,
        context.export_process_clear_s3_manifests,
    )


@given("The export process is performed for snapshot type of '{snapshot_type}'")
@then("The export process is performed for snapshot type of '{snapshot_type}'")
def step_impl(context, snapshot_type):
    message_helper.send_start_export_message(
        context.aws_sns_uc_ecc_arn,
        context.test_run_name,
        context.topics_delimited,
        None,
        None,
        context.test_run_name,
        "false",
        False,
        snapshot_type,
        snapshots_helper.get_snapshot_run_correlation_id(
            context.test_run_name, snapshot_type
        ),
        context.export_process_trigger_adg_override,
        context.generate_snapshots_export_date_override,
        context.export_process_clear_s3_snapshots,
        context.export_process_clear_s3_manifests,
    )


@when(
    "The export process is performed on the unique topics with start time of '{start_timestamp}', end time of '{end_timestamp}' and snapshot type of '{snapshot_type}'"
)
def step_impl(context, start_timestamp, end_timestamp, snapshot_type):
    message_helper.send_start_export_message(
        context.aws_sns_uc_ecc_arn,
        context.test_run_name,
        context.topics_unique_delimited,
        start_timestamp,
        end_timestamp,
        context.test_run_name,
        "true",
        context.generate_snapshots_reprocess_files,
        snapshot_type,
        snapshots_helper.get_snapshot_run_correlation_id(
            context.test_run_name, snapshot_type
        ),
        context.export_process_trigger_adg_override,
        context.generate_snapshots_export_date_override,
        context.export_process_clear_s3_snapshots,
        context.export_process_clear_s3_manifests,
    )


@when(
    "The dynamodb messages for each topic are one of '{statuses}' for snapshot type of '{snapshot_type}'"
)
@then(
    "The dynamodb messages for each topic are one of '{statuses}' for snapshot type of '{snapshot_type}'"
)
def step_impl(context, statuses, snapshot_type):
    topics = message_helper.get_consolidated_topics_list(
        [topic["topic"] for topic in context.topics_for_test],
        snapshot_type,
        context.default_topic_list_full_delimited,
        context.default_topic_list_incremental_delimited,
        context.default_topic_list_drift_testing_incrementals,
        [
            context.send_snapshots_topics_override,
            context.generate_snapshots_topics_override,
        ],
    )
    correlation_id = (
        snapshots_helper.get_snapshot_run_correlation_id(
            context.test_run_name, snapshot_type
        )
        if not context.send_snapshots_correlation_id_override
        else context.send_snapshots_correlation_id_override
    )

    status_list = [statuses] if "," not in statuses else statuses.split(",")
    if not export_status_helper.wait_for_statuses_in_export_status_table(
        context.timeout,
        context.dynamo_db_export_status_table_name,
        topics,
        correlation_id,
        status_list,
    ):
        raise AssertionError("Statuses have not been set to sent")


@when(
    "The dynamodb status for '{product}' is set to '{expected}' with snapshot type of '{snapshot_type}'"
)
@then(
    "The dynamodb status for '{product}' is set to '{expected}' with snapshot type of '{snapshot_type}'"
)
def step_impl(context, product, expected, snapshot_type):
    correlation_id_override = None
    if context.send_snapshots_correlation_id_override:
        correlation_id_override = context.send_snapshots_correlation_id_override
    elif context.generate_snapshots_correlation_id_override:
        correlation_id_override = context.generate_snapshots_correlation_id_override

    correlation_id = (
        snapshots_helper.get_snapshot_run_correlation_id(
            context.test_run_name, snapshot_type
        )
        if correlation_id_override is None
        else correlation_id_override
    )

    console_printer.print_debug(
        f"Getting DynamoDb data from product status table for correlation id of '{correlation_id}', product of '{product}' and table name of '{context.dynamo_db_product_status_table_name}'"
    )

    response = data_pipeline_metadata_helper.get_item_from_product_status_table(
        context.dynamo_db_product_status_table_name,
        product,
        correlation_id,
    )

    assert (
        response is not None
    ), f"Could not retrieve status row for product '{product}' and correlation id '{correlation_id}'"
    assert (
        "Item" in response
    ), f"Could not retrieve status row item for product '{product}' and correlation id '{correlation_id}'"

    item = response["Item"]
    assert "Status" in item, f"Could not retrieve status dynamodb from item '{item}'"

    actual = item["Status"]["S"]
    assert (
        expected == actual
    ), f"Actual status of '{actual}' is not the same as the expected status of '{expected}'"
