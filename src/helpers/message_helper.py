from helpers import aws_helper


def send_start_import_message(
    queue,
    s3_suffix,
    skip_earlier_than,
    skip_later_than,
    test_run_name,
    run_import=True,
    generate_manifest=False,
    skip_existing_records=True,
    correlation_id=None,
):
    """Sends the relevant SQS message for starting the importer.

    Keyword arguments:
    queue -- the queue to send the message to
    s3_suffix -- the suffix to pass in SQS (or None)
    skip_earlier_than -- the starting cut off time for imports to pass in SQS (or None)
    skip_later_than -- the ending cut off time for imports to pass in SQS (or None)
    test_run_name -- the test run name
    run_import -- False to not run import
    generate_manifest -- True to also generate the manifest
    skip_existing_records -- True to skip records already in HBase, False to insert anyway
    correlation_id -- Correlation Id override or None for a uuid
    """
    s3_suffix_command = (
        '"s3-suffix":"' + s3_suffix + '", ' if s3_suffix is not None else ""
    )

    skip_earlier_than_command = (
        '"skip-earlier-than":"' + skip_earlier_than + '", '
        if skip_earlier_than is not None
        else ""
    )

    skip_later_than_command = (
        '"skip-later-than":"' + skip_later_than + '", '
        if skip_later_than is not None
        else ""
    )

    correlation_id_command = (
        '"correlation-id-override":"' + correlation_id + '", '
        if correlation_id is not None
        else ""
    )

    run_import_command = "true" if run_import else "false"

    generate_manifest_command = "true" if generate_manifest else "false"

    skip_existing_records_command = "true" if skip_existing_records else "false"

    message_body = (
        '{"run-import":"'
        + f"{run_import_command}"
        + '", '
        + f"{s3_suffix_command}"
        + f"{skip_earlier_than_command}"
        + f"{skip_later_than_command}"
        + f"{correlation_id_command}"
        + '"generate-manifest":"'
        + f"{generate_manifest_command}"
        + '", '
        + '"skip-existing-records":"'
        + f"{skip_existing_records_command}"
        + '", '
        + '"hdi-shutdown-on-completion":"true", '
        + f'"message-source":"{test_run_name}"'
        + "}"
    )

    aws_helper.send_message_to_sqs(queue, message_body)


def send_start_export_message(
    uc_export_to_crown_controller_messages_sns_arn,
    s3_suffix,
    topics_override,
    start_time_override,
    end_time_override,
    test_run_name,
    run_ss_after_export,
    ss_reprocess_files,
    snapshot_type,
    correlation_id,
    trigger_adg_string,
    export_date_override,
    clear_s3_snapshots,
    clear_s3_manifests,
    mongo_snapshot_full_s3_location=None,
):
    """Sends the relevant SNS message for starting the exporter.

    Keyword arguments:
    uc_export_to_crown_controller_messages_sns_arn -- the htme controller sns topic arn
    s3_suffix -- the suffix to pass (or None)
    topics_override -- the active topics to pass (or None)
    start_time_override -- the time to get records with timestamps from (or None for all records)
    end_time_override -- the time to get records with timestamps to (or None for all records)
    test_run_name -- the full name of this test run
    run_ss_after_export -- True to run snapshot sender when export finished
    ss_reprocess_files -- True to tell snapshot sender to reprocess files if they already exist
    snapshot_type -- either "full" or "incremental"
    correlation_id -- Correlation Id override or None for a uuid
    trigger_adg_string -- True for HTME to trigger ADG when finished (defaults to False)
    export_date_override -- Correlation Id override or None for not sending, which means using today's date
    clear_s3_snapshots -- True for HTME to delete any existing snapshots before it runs (defaults to False)
    clear_s3_manifests -- True for HTME to delete any existing manifests before it runs (defaults to False)
    mongo_snapshot_full_s3_location -- full location for the snapshots (or None if not needed)
    """
    reprocess_files = "true" if ss_reprocess_files else "false"

    message = {
        "run-export": "true",
        "htme-shutdown-on-completion": "true",
        "ss-shutdown-on-completion": "true",
        "message-source": test_run_name,
        "ss-reprocess-files": reprocess_files,
        "snapshot-type": snapshot_type,
    }

    if mongo_snapshot_full_s3_location is not None:
        message["mongo_snapshot_full_s3_location"] = mongo_snapshot_full_s3_location

    if s3_suffix is not None:
        message["s3-suffix"] = s3_suffix

    if run_ss_after_export is not None and run_ss_after_export != "":
        message["run-ss-after-export"] = run_ss_after_export

    if topics_override is not None:
        message["topic-override"] = topics_override

    if start_time_override is not None:
        message["start-date-time"] = start_time_override

    if end_time_override is not None:
        message["end-date-time"] = end_time_override

    if correlation_id is not None:
        message["correlation-id-override"] = correlation_id

    if trigger_adg_string is not None:
        message["trigger-adg"] = trigger_adg_string.lower()

    if export_date_override:
        message["export-date"] = export_date_override,

    if clear_s3_snapshots is not None:
        message["clear-s3-snapshots"] = clear_s3_snapshots.lower()

    if clear_s3_manifests is not None:
        message["clear-s3-manifests"] = clear_s3_manifests.lower()

    return aws_helper.publish_message_to_sns(
        message, uc_export_to_crown_controller_messages_sns_arn
    )


def send_start_snapshot_sending_message(
    snapshot_sender_sqs_queue,
    s3_full_folder,
    topic_name,
    correlation_id,
    reprocess_files,
    export_date,
    snapshot_type,
):
    """Sends the relevant SNS message for starting the exporter.

    Keyword arguments:
    snapshot_sender_sqs_queue -- the htme controller sns topic arn
    s3_full_folder -- the full s3 location of the snapshots
    topic_name -- the topic name
    correlation_id -- the test run name
    reprocess_files -- boolean to reprocess files or not
    export_date -- the date the files were exported
    snapshot_type -- full or incremental for the type of the snapshots
    """
    reprocess_files_value = "true" if reprocess_files else "false"

    message_body = (
        '{"shutdown_flag":"'
        + f"true"
        + '", '
        + '"correlation_id":"'
        + f"{correlation_id}"
        + '", '
        + '"topic_name":"'
        + f"{topic_name}"
        + '", '
        + '"reprocess_files":"'
        + f"{reprocess_files_value}"
        + '", '
        + '"export_date":"'
        + f"{export_date}"
        + '", '
        + '"s3_full_folder":"'
        + s3_full_folder
        + '", '
        + '"snapshot_type":"'
        + snapshot_type
        + '"}'
    )

    aws_helper.send_message_to_sqs(snapshot_sender_sqs_queue, message_body)


def get_consolidated_topics_list(
    fallback_topics,
    snapshot_type,
    default_topic_list_full,
    default_topic_list_incremental,
    topics_overrides=None,
):
    """Works out the topics needed for snapshot sender based on the passed in overrides.

    Keyword arguments:
    fallback_topics -- the topics for this tests set by the environment hook (used if no override)
    snapshot_type -- either "full" or "incremental" if using the default topic lists (defaults to "full")
    default_topic_list_full -- if snapshot_type is "full" then this comma delimited list is used for the topics
    default_topic_list_incremental -- if snapshot_type is "incremental" then this comma delimited list is used for the topics
    topics_override -- if provided as an array of comma delimited lists, the first valid one is used for the topics
    """
    if topics_overrides is not None:
        for topics_override in topics_overrides:
            if topics_override:
                if topics_override.lower() == "all":
                    return (
                        default_topic_list_incremental.split(",")
                        if snapshot_type.lower() == "incremental"
                        else default_topic_list_full.split(",")
                    )
                else:
                    return topics_override.split(",")

    return fallback_topics
