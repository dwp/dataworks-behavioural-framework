import base64
import json
from datetime import datetime

from helpers import aws_helper, dataworks_kafka_producer_helper, console_printer


def create_data_file(message_count):
    data = []
    for journal_id in range(message_count):
        element = json_element(journal_id)
        data.append(element)

    data_str = f"[{','.join(data)}]"
    return data_str


def json_element(journal_id):
    element = {
        "journal_id": str(journal_id),
        "flagged": "yes",
        "timestamp": datetime.today().strftime('%d%m%Y'),
        "modelIdentifier": "v1",
        "modelRunDate": "01062021"
    }

    return json.dumps(element)


def upload_file_to_s3(context, data_obj, data_key, file_counter):
    # Encrypt the data
    (encrypted_data, iv,) = dataworks_kafka_producer_helper.encrypt_data_aes_ctr(
        plaintext_string=str(data_obj), data_key=data_key
    )

    # Set metadata
    metadata = {
        "iv": iv,
        "ciphertext": context.encryption_encrypted_key,
        "datakeyencryptionkeyid": context.encryption_master_key_id,
    }

    # s3 object key
    object_key = f"{context.dataworks_model_output_s3_prefix}/perf_test_data-{str(file_counter)}.json"
    console_printer.print_info(f"Uploading file to S3: {object_key}")
    # Upload the data to s3
    aws_helper.put_object_in_s3_with_metadata(
        body=encrypted_data,
        s3_bucket=context.dataworks_model_output_s3_bucket,
        metadata=metadata,
        s3_key=object_key,
    )
    console_printer.print_info(f"Uploading file to S3: {object_key}...complete")


def get_message_count_old(context):
    client = aws_helper.get_client('sqs')
    queue_url = client.get_queue_url(QueueName=context.dataworks_model_sqs_queue)['QueueUrl']
    response = client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=['All'])
    message_count = response['Attributes']['ApproximateNumberOfMessages']
    return message_count

def get_message_count(context):
    instance_id = aws_helper.get_instance_id("dataworks-kafka-producer")

    linux_command = "sh /home/ec2-user/kafka/utils/run_get_topic_last_offset.sh"
    response = aws_helper.execute_linux_command(instance_id, linux_command)
    message_count = response["StandardOutputContent"].rstrip()
    return int(message_count)
