from behave import given, then
import os
import zlib
import time
import base64
from helpers import (
    historic_data_load_generator,
    aws_helper,
    console_printer,
    snapshots_helper,
)

OUTPUT_FILE_REGEX = r".*.txt"
IV = "iv"
DATAENCRYPTIONKEYID = "datakeyencryptionkeyid"
CIPHERTEXT = "ciphertext"
S3_PREFIX_FOR_INPUT = "dataworks-egress-testing-input/"
S3_PREFIX_FOR_SFT_INPUT = "dataworks-egress-testing-sft-input/"
S3_PREFIX_FOR_OUTPUT = "data-egress-testing-output/"
S3_PREFIX_FOR_SFT_OUTPUT = "sft/"
TEMPLATE_FOLDER = "data_egress_data"
TEMPLATE_SUCCESS_FILE = "pipeline_success.flag"


@given(
    "the data in file '{template_name}' written to '{file_location}'"
)
def step_prepare_sft_test(context, template_name, file_location):
    aws_helper.clear_s3_prefix(
    context.snapshot_s3_output_bucket,
    S3_PREFIX_FOR_SFT_OUTPUT,
    False
    )

    # ec2_client = aws_helper.get_client('ec2')
 
    # custom_filter = [{
    # 'Name':'tag:Application', 
    # 'Values': ['dataworks-aws-data-egress']}]
    
    # response = ec2_client.describe_instances(Filters=custom_filter)
    # console_printer.print_info(f"Response from describe instances {response}")

    console_printer.print_info(f"Executing commands on Ec2")
    ssm_client = aws_helper.get_client('ssm')
    commands = ['echo "hello world"', 'sudo su', 'cd /data-egress', 'echo "test" >> test1.txt']
    # instance_ids = ['', '']
    resp = ssm_client.send_command(
        DocumentName="AWS-RunShellScript", 
        Parameters={'commands': commands},
        TimeoutSeconds=30,
        Targets=[{
            'Key': 'tag:Application',
            'Values': [
                'dataworks-aws-data-egress'
            ]
        }]
    )
    console_printer.print_info(f"Response from ssm {resp}")

    console_printer.print_info(f"CommandId from ssm request {resp['Command']['CommandId']}")
    time.sleep(30)
    status = ssm_client.list_commands(
        CommandId=resp['Command']['CommandId']
    )
    console_printer.print_info(f"Response from ssm status {status}")
    console_printer.print_info(f"Response from ssm status {status} 2")

@given(
    "the data in file '{template_name}' encrypted using DKS and uploaded to S3 bucket"
)
def step_prepare_data_egress_test(context, template_name):
    template_file = os.path.join(
        context.fixture_path_local, TEMPLATE_FOLDER, template_name
    )
    with open(template_file, "r") as unencrypted_file:
        unencrypted_content = unencrypted_file.read()
    [
        iv_int,
        iv_whole,
    ] = historic_data_load_generator.generate_initialisation_vector()
    iv = base64.b64encode(iv_int).decode()
    encrypted_content = historic_data_load_generator.generate_encrypted_record(
        iv_whole, unencrypted_content, context.encryption_plaintext_key, False
    )
    file_name = os.path.basename(template_file)
    s3_key = os.path.join(S3_PREFIX_FOR_INPUT, file_name)
    metadata = {
        CIPHERTEXT: context.encryption_encrypted_key,
        DATAENCRYPTIONKEYID: context.encryption_master_key_id,
        IV: iv,
    }

    console_printer.print_info(f"Uploading DKS encrypted file to S3")
    aws_helper.put_object_in_s3_with_metadata(
        encrypted_content,
        context.published_bucket,
        s3_key,
        metadata,
    )
    console_printer.print_info(f"Uploading success file to S3")

    console_printer.print_info(f"Uploading success file to S3")
    template_success_file = os.path.join(
        context.fixture_path_local, TEMPLATE_FOLDER, TEMPLATE_SUCCESS_FILE
    )
    success_file_key = os.path.join(S3_PREFIX_FOR_INPUT, TEMPLATE_SUCCESS_FILE)
    aws_helper.upload_file_to_s3_and_wait_for_consistency(
        template_success_file, context.published_bucket, 10, success_file_key
    )


@then("verify content of the data egress output file")
def step_verify_data_egress_content(context):
    time.sleep(10)
    keys = aws_helper.get_s3_file_object_keys_matching_pattern(
        context.published_bucket, S3_PREFIX_FOR_OUTPUT, OUTPUT_FILE_REGEX
    )
    console_printer.print_info(f"Keys in data egress output location : {keys}")
    console_printer.print_info(f"Keys in data egress output location : {keys} 2")
    assert len(keys) == 1
    output_file_content = aws_helper.get_s3_object(
        bucket=context.published_bucket, key=keys[0], s3_client=None
    ).decode()
    console_printer.print_info(f"file content is : {output_file_content}")
    console_printer.print_info(f"file content is : {output_file_content} 2")
    assert (
        output_file_content == "This is just sample data to test data egress service."
    )

@then("verify content of the SFT output file")
def step_verify_stf_content(context):
    time.sleep(10)
    keys = aws_helper.get_s3_file_object_keys_matching_pattern(
        context.snapshot_s3_output_bucket, S3_PREFIX_FOR_SFT_OUTPUT, OUTPUT_FILE_REGEX
    )

    console_printer.print_info(f"Keys in data egress SFT output location : {keys}")
    assert len(keys) == 1
    output_file_content = aws_helper.get_s3_object(
        bucket=context.snapshot_s3_output_bucket, key=keys[0], s3_client=None
    ).decode()
    console_printer.print_info(f"sft file content is : {output_file_content}")
    console_printer.print_info(f"sft file content is : {output_file_content} 2")
    assert (
        output_file_content == "This is just sample data to test data egress service."
    )




