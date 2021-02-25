from behave import given, when, then
import os
import time
import json
from helpers import (
    template_helper,
    aws_helper,
    export_status_helper,
    console_printer,
    message_helper,
)
from helpers import (
    snapshot_data_generator,
    file_comparer,
    file_helper,
    snapshots_helper,
)


@when(
    "The snapshot sending process is performed with default settings for snapshot type of '{snapshot_type}'"
)
def step_impl(context, snapshot_type):
    topics = message_helper.get_consolidated_topics_list(
        [topic["topic"] for topic in context.topics_for_test],
        snapshot_type,
        context.default_topic_list_full_delimited,
        context.default_topic_list_incremental_delimited,
        [context.send_snapshots_topics_override],
    )

    formatted_date = (
        context.formatted_date
        if not context.send_snapshots_date_override
        else context.send_snapshots_date_override
    )
    correlation_id = (
        snapshots_helper.get_snapshot_run_correlation_id(
            context.test_run_name, snapshot_type
        )
        if not context.send_snapshots_correlation_id_override
        else context.send_snapshots_correlation_id_override
    )
    reprocess_files = context.send_snapshots_reprocess_files
    s3_qualified_prefix = os.path.join(
        context.mongo_snapshot_path, formatted_date, snapshot_type
    )

    console_printer.print_info(
        f"Looking for snapshots present in '{s3_qualified_prefix}'"
    )

    for topic in topics:
        topic_qualified = template_helper.get_topic_name(topic)
        topic_name = template_helper.remove_any_pipe_values_from_topic_name(topic_qualified)

        snapshot_pattern = f"^{s3_qualified_prefix}/{topic_name}-\d{{3}}-\d{{3}}-\d+.txt.gz.enc$"

        console_printer.print_info(
            f"Looking for snapshots using pattern '{snapshot_pattern}'"
        )

        generated_snapshot_keys = aws_helper.get_s3_file_object_keys_matching_pattern(
            context.mongo_snapshot_bucket,
            f"{s3_qualified_prefix}/{topic_name}-",
            snapshot_pattern,
        )

        generated_snapshots_count = len(generated_snapshot_keys)

        console_printer.print_info(
            f"Found '{generated_snapshots_count}' snapshots"
        )

        export_status_helper.update_item_in_export_status_table(
            context.dynamo_db_export_status_table_name,
            topic_name,
            correlation_id,
            "Exported",
            generated_snapshots_count,
            0,
            0,
        )

        for generated_snapshot_key in generated_snapshot_keys:
            message_helper.send_start_snapshot_sending_message(
                context.aws_sqs_queue_snapshot_sender,
                generated_snapshot_key,
                topic_name,
                correlation_id,
                reprocess_files,
                formatted_date,
                snapshot_type,
            )


@then(
    "The number of snapshots created for each topic is '{number_of_snapshots}' with match type of '{match_type}' and snapshot type of '{snapshot_type}'"
)
def step_impl(context, number_of_snapshots, match_type, snapshot_type):
    s3_qualified_prefix = os.path.join(
        context.mongo_snapshot_path,
        context.test_run_name,
        context.formatted_date,
        snapshot_type,
    )
    topic_names = [
        template_helper.get_topic_name(topic["topic"])
        for topic in context.topics_for_test
    ]

    snapshot_count = int(number_of_snapshots)

    if snapshot_count == 0:
        for result in aws_helper.assert_no_snapshots_in_s3_threaded(
            topic_names, context.mongo_snapshot_bucket, s3_qualified_prefix, 60
        ):
            console_printer.print_info(
                f"Asserted no snapshots created in s3 with key of {result}"
            )
    else:
        snapshot_string = "snapshots" if snapshot_count > 1 else "snapshot"
        for result in aws_helper.assert_snapshots_in_s3_threaded(
            topic_names,
            context.mongo_snapshot_bucket,
            s3_qualified_prefix,
            snapshot_count,
            context.timeout,
            ("exact" == match_type),
        ):
            console_printer.print_info(
                f"Asserted exactly {number_of_snapshots} {snapshot_string} created in s3 with key of {result}"
            )


@then(
    "Snapshot sender sends the correct snapshots for snapshot type of '{snapshot_type}'"
)
def step_impl(context, snapshot_type):
    topic_names = [
        template_helper.get_topic_name(topic["topic"])
        for topic in context.topics_for_test
    ]

    for topic_name in topic_names:
        console_printer.print_info(f"Checking snapshots for topic '{topic_name}'")

        generated_file = (
            snapshot_data_generator.generate_snapshot_file_from_hbase_records(
                context.test_run_name,
                topic_name,
                context.snapshot_files_hbase_records_temp_folder,
                context.snapshot_files_temp_folder,
            )
        )

        expected_records = snapshots_helper.get_locally_generated_snapshot_file_records(
            generated_file
        )
        expected_records.sort()

        console_printer.print_info(
            f"Checking snapshots from s3 at base path '{context.snapshot_s3_output_path}' "
            + f"with db name of '{context.db_name}', topic name of '{topic_name}' and date of '{context.formatted_date}'"
        )

        s3_qualified_prefix = os.path.join(
            context.mongo_snapshot_path,
            context.test_run_name,
            context.formatted_date,
            snapshot_type,
        )

        console_printer.print_info(
            f"Checking generated snapshots from s3 at path '{s3_qualified_prefix}'"
        )

        generated_snapshots_count = len(
            aws_helper.get_s3_file_object_keys_matching_pattern(
                context.mongo_snapshot_bucket,
                s3_qualified_prefix,
                f"^{s3_qualified_prefix}/{topic_name}-\d{{3}}-\d{{3}}-\d+.txt.gz.enc$",
            )
        )

        console_printer.print_info(
            f"Found '{generated_snapshots_count}' generated snapshots from s3 at path '{s3_qualified_prefix}'"
        )

        snapshot_s3_full_output_path = (
            snapshots_helper.generate_snapshot_output_s3_path(
                context.snapshot_s3_output_path,
                topic_name,
                context.db_name,
                context.formatted_date,
                snapshot_type,
            )
        )

        console_printer.print_info(
            f"Checking snapshots from s3 at path '{snapshot_s3_full_output_path}'"
        )

        if not snapshots_helper.wait_for_snapshots_to_be_sent_to_s3(
            context.timeout,
            generated_snapshots_count,
            context.snapshot_s3_output_bucket,
            snapshot_s3_full_output_path,
        ):
            raise AssertionError(
                f"Snapshots found at '{snapshot_s3_full_output_path}' was not above or matching expected minimum of '{generated_snapshots_count}'"
            )

        console_printer.print_info(
            f"Length of the expected record array is '{len(expected_records)}'"
        )

        console_printer.print_info(
            f"Getting hbase records from snapshots from s3 at path '{snapshot_s3_full_output_path}'"
        )

        actual_records = snapshots_helper.retrieve_records_from_snapshots(
            context.snapshot_s3_output_bucket, snapshot_s3_full_output_path
        )
        actual_records.sort()
        console_printer.print_info(
            f"Length of the actual record array is '{len(actual_records)}'"
        )

        console_printer.print_info("Asserting the length of the two record arrays")
        console_printer.print_info(f"Expected: {expected_records}")
        console_printer.print_info(f"Actual: {actual_records}")
        assert len(actual_records) >= len(expected_records)

        missing_snapshots = []
        console_printer.print_info("Asserting the values of the expected snapshots")

        for expected_record_number in range(0, len(expected_records)):
            console_printer.print_info(
                f"Asserting the values of expected record number '{expected_record_number}'"
            )

            expected_json = json.loads(expected_records[expected_record_number])

            record_found = False
            for actual_record_number in range(0, len(actual_records)):
                actual_json = json.loads(actual_records[actual_record_number])

                if expected_json == actual_json:
                    console_printer.print_info(
                        f"Expected json is '{expected_json}' and actual json (record number '{actual_record_number}') is '{actual_json}'"
                    )
                    record_found = True
                    break

            if not record_found:
                missing_snapshots.append(expected_record_number)

        if len(missing_snapshots) > 0:
            console_printer.print_info(
                f"The following snapshots were not found: '{missing_snapshots}'"
            )

        console_printer.print_info("Individual assertions complete")
        console_printer.print_info("Asserting no records are mismatched")

        assert len(missing_snapshots) == 0
