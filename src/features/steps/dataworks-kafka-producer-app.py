import base64
import os

import time
from behave import given, when, then
from helpers import aws_helper, dataworks_kafka_producer_helper, console_printer


@given("the e2e kafka producer app is running")
def step_impl(context):
    # Get instance id
    instance_id = aws_helper.get_instance_id("dataworks-kafka-producer")

    # Start kafka producer
    linux_command = "nohup sh /home/ec2-user/kafka/run_e2e.sh &"
    aws_helper.execute_linux_command(instance_id, linux_command)


@when("a json file '{file_name}' is uploaded to S3 location")
def step_impl(context, file_name):
    # Read the data file
    plaintext_string = dataworks_kafka_producer_helper.read_test_data(file_name)

    # s3 object key
    object_key = f"{context.dataworks_model_output_s3_prefix}/{file_name}"
    console_printer.print_info(f"Uploading file to S3: {object_key}")

    aws_helper.put_object_in_s3(
        body=plaintext_string,
        s3_bucket=context.dataworks_model_output_s3_bucket,
        s3_key=object_key,
    )

    # Wait for the file to be processed
    time.sleep(30)


@then("the last offset should be incremented by '{expected_lag}'")
def step_impl(context, expected_lag):
    # Get instance id
    instance_id = aws_helper.get_instance_id("dataworks-kafka-producer")

    linux_command = "sh /home/ec2-user/kafka/utils/run_get_topic_last_offset.sh"
    response = aws_helper.execute_linux_command(instance_id, linux_command)
    actual_lag = response["StandardOutputContent"].rstrip()
    msg = f"Mismatch in actual ({actual_lag}) and expected lag ({expected_lag}).\nResponse:\t{response}\nStandardOutputContent:\t{response['StandardOutputContent']}"
    assert actual_lag == expected_lag, msg
