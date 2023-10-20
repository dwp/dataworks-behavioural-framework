from behave import given, when, then
import time
import json
from helpers import (
    aws_helper,
    console_printer,
)


S3_PREFIX_FOR_SFT_OUTPUT = "e2e/data-egress/txr/"


@given("a set of collections")
def step_prepare_sft_test(context):
    context.txr_test_collections = []

    for row in context.table:
        context.txr_test_collection.append(
            {
                "name": row["name"],
                "file": f"{row['name']}-123-123-123456.12345678.txt.gz.enc",
                "destination": row["destination"],
            }
        )

    # remove all sft files currently in the stub nifi output bucket
    aws_helper.clear_s3_prefix(
        context.data_ingress_stage_bucket, S3_PREFIX_FOR_SFT_OUTPUT, True
    )


@when("we submit them to '{data_directory}' data directory on the SFT service")
def step_submit_files_to_sft(context, data_directory):
    console_printer.print_info(f"Executing commands on Ec2")
    for collection in context.txt_test_collections:
        commands = [
            "sudo su",
            f"cd /var/lib/docker/volumes/data-egress/_data/{data_directory}",
            f"echo 'test content' >> {collection['file']}",
        ]
        aws_helper.execute_commands_on_ec2_by_tags_and_wait(
            commands, ["dataworks-aws-data-egress"], 30
        )


@then("we verify the collection files are correctly distributed in S3")
def step_verify_stf_content(context):
    time.sleep(5)

    for collection in context.txt_test_collections:
        console_printer.print_info(f"asserting against : {collection['name']}")
        output_file_content = (
            aws_helper.get_s3_object(
                bucket=context.data_ingress_stage_bucket,
                key=f"{S3_PREFIX_FOR_SFT_OUTPUT}{collection['destination']}/{collection['file']}",
                s3_client=None,
            )
            .decode()
            .strip()
        )

        console_printer.print_info(f"sft file content is : {output_file_content}")
        assert output_file_content == "test content"
