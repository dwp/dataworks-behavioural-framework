from behave import given, when, then
import os
import time
import json
from helpers import (
    template_helper,
    file_helper,
    metadata_store_helper,
    console_printer,
    streaming_data_helper,
)


@then(
    "The latest id from the Kafka dlq file '{dlq_file_template}' has not been written to the '{table}' metadata table with id format of '{id_format}'"
)
def step_impl(context, dlq_file_template, table, id_format):
    if context.data_streaming_tests_skip_reconciling:
        console_printer.print_warning_text(
            f"Not verifying reconciliation due to DATA_STREAMING_TESTS_SKIP_RECONCILING being set to '{str(context.data_streaming_tests_skip_reconciling)}'"
        )
        return

    wrap_id_value = id_format == "wrapped"
    table_name = streaming_data_helper.get_metadata_store_table_name(
        table, context.metadata_store_tables
    )

    for topic in context.topics_for_test:
        dlq_file = None
        for dlq_files_and_topic_tuple in context.kafka_generated_dlq_output_files:
            if topic["topic"] == dlq_files_and_topic_tuple[0]:
                for dlq_file_for_topic in dlq_files_and_topic_tuple[1]:
                    if dlq_file_template in dlq_file_for_topic:
                        dlq_file = dlq_file_for_topic

        if dlq_file is None:
            raise AssertionError(
                f"No generated dlq file could be found for dlq template of {dlq_file_template} in topic '{topic_name}'"
            )

        topic_name = template_helper.get_topic_name(topic["topic"])
        id_object = file_helper.get_id_object_from_json_file(dlq_file)
        id_string = json.dumps(id_object)

        if wrap_id_value:
            id_string = json.dumps({"id": id_string})

        id_string_qualified = id_string.replace(" ", "")

        results = metadata_store_helper.get_metadata_for_specific_id_in_topic(
            table_name, id_string_qualified, topic_name
        )

        console_printer.print_info(
            f"Received {len(results)} responses in topic '{topic_name}'"
        )
        console_printer.print_info(
            f"Actual metadata store results are '{results}' in topic '{topic_name}'"
        )
        console_printer.print_info(
            f"Asserting no response received for id of '{id_string_qualified}' in topic '{topic_name}'"
        )

        assert len(results) == 0


@then(
    "The latest id and timestamp have been correctly written to the '{table}' metadata table with id format of '{id_format}' for message type '{message_type}'"
)
def step_impl(context, table, id_format, message_type):
    if context.data_streaming_tests_skip_reconciling:
        console_printer.print_warning_text(
            f"Not verifying reconciliation due to DATA_STREAMING_TESTS_SKIP_RECONCILING being set to '{str(context.data_streaming_tests_skip_reconciling)}'"
        )
        return

    folder = streaming_data_helper.generate_fixture_data_folder(message_type)

    context.latest_metadata_store_ids = []
    wrap_id_value = id_format == "wrapped"
    table_name = streaming_data_helper.get_metadata_store_table_name(
        table, context.metadata_store_tables
    )

    for topic in context.topics_for_test:
        topic_name = template_helper.get_topic_name(topic["topic"])
        temp_folder_for_topic = os.path.join(context.temp_folder, topic_name)
        full_folder_path = file_helper.generate_edited_files_folder(
            temp_folder_for_topic, folder
        )
        latest_file_path = file_helper.get_file_from_folder_with_latest_timestamp(
            full_folder_path
        )

        (
            record_id,
            record_timestamp,
            results,
        ) = metadata_store_helper.get_metadata_for_id_and_timestamp_from_file(
            table_name, latest_file_path, topic_name, wrap_id_value
        )

        console_printer.print_info(
            f"Received {len(results)} responses in topic '{topic_name}'"
        )
        console_printer.print_info(
            f"Actual metadata store results are '{results}' in topic '{topic_name}'"
        )
        console_printer.print_info(
            f"Asserting exactly one response received for id of '{record_id}' and timestamp of '{str(record_timestamp)}' in topic '{topic_name}'"
        )

        assert (
            len(results) == 1
        ), "Metadata table result not returned, try restarting the k2hb consumers"

        results_iterator = iter(results.items())
        result_row_key_value_pair = next(results_iterator)
        result_row_key = result_row_key_value_pair[0]
        result_row_value = result_row_key_value_pair[1]

        console_printer.print_info(
            f"Asserting the key value for the result in topic '{topic_name}'"
        )

        assert record_id in result_row_key

        console_printer.print_info(
            f"Asserted key value of '{result_row_key}' contains expected id of '{record_id}' in topic '{topic_name}'"
        )
        console_printer.print_info(
            f"Asserting the field values for the result in topic '{topic_name}'"
        )

        assert record_id in result_row_value["hbase_id"]
        assert record_timestamp == result_row_value["hbase_timestamp"]
        assert topic_name == result_row_value["topic_name"]

        context.latest_metadata_store_ids.append(
            [topic_name, record_id, record_timestamp]
        )


@then(
    "The reconciler reconciles the latest id and timestamp to the '{table}' metadata table"
)
def step_impl(context, table):
    if context.data_streaming_tests_skip_reconciling:
        console_printer.print_warning_text(
            f"Not verifying reconciliation due to DATA_STREAMING_TESTS_SKIP_RECONCILING being set to '{str(context.data_streaming_tests_skip_reconciling)}'"
        )
        return

    table_name = streaming_data_helper.get_metadata_store_table_name(
        table, context.metadata_store_tables
    )

    timeout_seconds = 600
    console_printer.print_info(
        f"Checking that all ids are reconciled within '{str(timeout_seconds)}' seconds"
    )

    for latest_metadata_store_id_for_topic in context.latest_metadata_store_ids:
        reconciled = False
        timeout_time = time.time() + timeout_seconds
        while time.time() < timeout_time and reconciled == False:
            topic_name = template_helper.get_topic_name(
                latest_metadata_store_id_for_topic[0]
            )
            console_printer.print_info(
                f"Checking that latest id is reconciled for topic '{topic_name}'"
            )

            results = metadata_store_helper.get_metadata_for_specific_id_and_timestamp_in_topic(
                table_name,
                latest_metadata_store_id_for_topic[1],
                latest_metadata_store_id_for_topic[2],
                topic_name,
            )
            assert len(results) == 1

            results_iterator = iter(results.items())
            result_row_key_value_pair = next(results_iterator)
            result_row_key = result_row_key_value_pair[0]
            result_row_value = result_row_key_value_pair[1]

            console_printer.print_info(
                f"Actual metadata store result is '{result_row_value}' in topic '{topic_name}'"
            )
            console_printer.print_info(
                f"Asserting the reconciled field values for the result in topic '{topic_name}'"
            )

            if result_row_value["reconciled_result"] == 1:
                console_printer.print_info(
                    f"Asserting reconciled timestamp is set in topic '{topic_name}'"
                )
                assert result_row_value["reconciled_timestamp"] is not None

                console_printer.print_info(
                    f"Latest id has been reconciled for topic '{topic_name}'"
                )
                reconciled = True
                break

            time.sleep(5)
        if reconciled == False:
            raise AssertionError(
                f"Latest id is not reconciled for topic '{topic_name}' after '{str(timeout_seconds)}' seconds"
            )
