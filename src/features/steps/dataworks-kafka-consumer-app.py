import json

from behave import given, when, then
from helpers import aws_helper, dataworks_kafka_consumer_helper, console_printer


@given("the e2e kafka consumer app is running")
def step_impl(context):
    # Start kafka consumer is running
    linux_command = "nohup sh /home/ec2-user/kafka/run_e2e.sh &"
    aws_helper.execute_linux_command(
        context.dataworks_kafka_dlq_consumer_instance, linux_command
    )


@when("messages are published into the dlq topic using test data in '{file_name}'")
def step_impl(context, file_name):
    # Read the data file
    console_printer.print_info(f"Read test data")
    messages = dataworks_kafka_consumer_helper.read_test_data(file_name)

    # Simulate UC sending DLQ messages - Loop thru the data file and Publish messages in DLQ topic
    console_printer.print_info(f"Publishing {len(messages)} messages to dlq topic")
    for message in messages:
        encoded_data = json.dumps(message)
        message_str = json.dumps(encoded_data)
        # console_printer.print_info(f"Publish data: {message_str}")
        linux_command = (
            f"sh /home/ec2-user/kafka/utils/e2e_publish_msg.sh {message_str}"
        )
        response = aws_helper.execute_linux_command(
            instance_id=context.dataworks_kafka_dlq_consumer_instance,
            linux_command=linux_command,
        )


@then("the consumer should write '{expected_file_count}' files to the S3 bucket")
def step_impl(context, expected_file_count):
    # Get a list of parquet files from the dlq bucket
    actual_file_count = dataworks_kafka_consumer_helper.get_parquet_file_count(
        f"{context.dataworks_kafka_dlq_output_bucket}",
        f"{context.dataworks_dlq_output_s3_prefix}/",
    )

    # Count the parquet files in the s3 location
    assert actual_file_count == int(expected_file_count)
