import base64
import time

from behave import given, when, then
from datetime import datetime

from helpers import (
    aws_helper,
    dataworks_kafka_producer_perf_test_helper,
    dataworks_kafka_producer_common_helper,
    console_printer,
)


@given(
    "'{file_count}' encrypted json file(s) with '{messages_per_file}' messages are available in S3 location"
)
def step_impl(context, file_count, messages_per_file):
    # Initialise test scenario
    console_printer.print_info("Initialise scenario....")
    dataworks_kafka_producer_common_helper.dataworks_init_kafka_producer(context)
    console_printer.print_info("Initialise scenario....complete")

    # Generate test data object
    console_printer.print_info("Generating data file object")
    data_obj = dataworks_kafka_producer_perf_test_helper.create_data_file(
        int(messages_per_file)
    )
    console_printer.print_info(f"Generating data file object...complete")
    # Get data key
    data_key = base64.b64decode(context.encryption_plaintext_key)

    # Upload files to S3
    console_printer.print_info(
        f"Uploading files to S3: {context.dataworks_model_output_s3_bucket}"
    )
    for file_counter in range(int(file_count)):
        dataworks_kafka_producer_perf_test_helper.upload_file_to_s3(
            context=context,
            file_counter=file_counter,
            data_obj=data_obj,
            data_key=data_key,
        )
    console_printer.print_info("Uploading files to S3...complete")
    time.sleep(10)


@when("the kafka producer app is started")
def step_impl(context):
    # Get instance id
    instance_id = aws_helper.get_instance_id("dataworks-kafka-producer")

    # Start kafka producer
    linux_command = "nohup sh /home/ec2-user/kafka/run_e2e.sh &"
    aws_helper.execute_linux_command(instance_id, linux_command)
    context.start_ts = datetime.now()
    console_printer.print_info(
        f"Start time for processing files:{datetime.today().strftime('%d-%m-%Y %H:%M:%S')}"
    )
    time.sleep(10)


@then("the kafka topic should have '{expected_message_count}' messages")
def step_impl(context, expected_message_count):
    # Get message count
    actual_offset = dataworks_kafka_producer_perf_test_helper.get_message_count(context)
    console_printer.print_info(f"Messages in kafka topic: {actual_offset}")

    # Loop till all the messages have been processed. Offsets start from 0
    while actual_offset < int(expected_message_count) - 1:
        # Get message count
        actual_offset = dataworks_kafka_producer_perf_test_helper.get_message_count(
            context
        )
        console_printer.print_info(f"Messages in kafka topic: {actual_offset}")
        time.sleep(20)

    end_ts = datetime.now()

    # Compute time in seconds to process all the files
    time_taken = end_ts - context.start_ts
    console_printer.print_info(
        f"End time time for processing files:{datetime.today().strftime('%d-%m-%Y %H:%M:%S')}"
    )
    console_printer.print_info("All data files have now been processed")
    console_printer.print_info(f"Time taken to process: {time_taken}")
    time.sleep(10)

    # Stop application after tests have run
    dataworks_kafka_producer_common_helper.dataworks_stop_kafka_producer_app(context)
