from behave import given, then
import os
import zlib
import time
import base64
from helpers import (
    historic_data_load_generator,
    aws_helper,
    console_printer,
)

OUTPUT_FILE_REGEX = r".*.txt"
IV = "iv"
DATAENCRYPTIONKEYID = "datakeyencryptionkeyid"
CIPHERTEXT = "ciphertext"
S3_PREFIX_FOR_INPUT = "dataworks-egress-testing-input/"
S3_PREFIX_FOR_OUTPUT = "data-egress-testing-output/"
S3_PREFIX_FOR_SFT_OUTPUT = "e2e/data-egress/startup-test/"
TEMPLATE_FOLDER = "data_egress_data"
TEMPLATE_SUCCESS_FILE = "pipeline_success.flag"


@given("the data in file '{template_name}' written to '{file_location}' directory")
def step_prepare_sft_test(context, template_name, file_location):
    # remove all sft files currently in the stub nifi output bucket
    aws_helper.clear_s3_prefix(
        context.data_ingress_stage_bucket, S3_PREFIX_FOR_SFT_OUTPUT, False
    )

    template_file = os.path.join(
        context.fixture_path_local, TEMPLATE_FOLDER, template_name
    )
    with open(template_file, "r") as unencrypted_file:
        unencrypted_content = unencrypted_file.read()

    console_printer.print_info(f"Executing commands on Ec2")
    commands = [
        "sudo su",
        f"cd /var/lib/docker/volumes/data-egress/_data/{file_location}",
        f"echo {unencrypted_content} >> {template_name}",
    ]
    aws_helper.execute_commands_on_ec2_by_tags_and_wait(
        commands, ["dataworks-aws-data-egress"], 30
    )


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
    aws_helper.clear_s3_prefix(context.published_bucket, S3_PREFIX_FOR_INPUT, False)

    console_printer.print_info(f"Uploading DKS encrypted file to S3")
    aws_helper.put_object_in_s3_with_metadata(
        encrypted_content,
        context.published_bucket,
        s3_key,
        metadata,
    )

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


@then("verify content of the SFT output file '{template_name}'")
def step_verify_stf_content(context, template_name):
    time.sleep(5)

    output_file_content = (
        aws_helper.get_s3_object(
            bucket=context.data_ingress_stage_bucket,
            key=f"{S3_PREFIX_FOR_SFT_OUTPUT}{template_name}",
            s3_client=None,
        )
        .decode()
        .strip()
    )

    console_printer.print_info(f"sft file content is : {output_file_content}")
    assert (
        output_file_content == "This is just sample data to test data egress service."
    )
