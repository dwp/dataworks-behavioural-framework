import os

import time
from behave import given, when, then
from helpers import aws_helper, dataworks_kafka_producer_helper, console_printer


@given("the e2e kafka producer app is running")
def step_impl(context):
    # Start kafka producer is running
    linux_command = "nohup sh /home/ec2-user/kafka/run_e2e.sh &"
    aws_helper.execute_linux_command(
        context.dataworks_kafka_producer_instance, linux_command
    )


@when("an encrypted json file '{file_name}' is uploaded to S3 location")
def step_impl(context, file_name):
    # Read the data file
    plaintext_string = dataworks_kafka_producer_helper.read_test_data(file_name)

    # Create a new data key for encrypting data
    (
        data_key,
        hsm_pub_key,
        encrypted_data_key,
    ) = dataworks_kafka_producer_helper.create_new_data_key()

    # Encrypt the data
    (
        encrypted_data,
        iv,
        encryption_data_key_id,
    ) = dataworks_kafka_producer_helper.encrypt_data_aes_ctr(
        plaintext_string=plaintext_string, data_key=data_key
    )

    # Set metadata
    metadata = {
        "iv": iv,
        "ciphertext": encrypted_data_key.decode("utf8"),
        "datakeyencryptionkeyid": encryption_data_key_id,
    }

    # s3 object key
    object_key = f"{context.dataworks_model_output_s3_prefix}/{file_name}"

    # Upload the data to s3
    aws_helper.upload_file_to_s3(
        body=encrypted_data,
        bucket=context.dataworks_model_output_s3_bucket,
        metadata=metadata,
        object_key=object_key,
    )

    # Wait for the file to be processed
    time.sleep(30)


@then("the consumer group lag should be incremented by '{expected_lag}'")
def step_impl(context, expected_lag):
    linux_command = "sh /home/ec2-user/kafka/utils/run_get_topic_last_offset.sh"
    response = aws_helper.execute_linux_command(
        context.dataworks_kafka_producer_instance, linux_command
    )
    actual_lag = response["StandardOutputContent"].rstrip()
    assert actual_lag == expected_lag
