from behave import given, when, then
from botocore.exceptions import ClientError
import time
from helpers import (
    aws_helper,
    console_printer,
)


S3_PREFIX_FOR_SFT_OUTPUT = "e2e/data-egress/txr/"


@given("a set of collections")
def step_prepare_sft_test(context):
    context.txr_test_collections = []

    for row in context.table:
        context.txr_test_collections.append(
            {
                "name": row["name"],
                "file": f"{row['name']}-123-123-123456.12345678.txt.gz.enc",
                "controlfile": f"{row['name']}-12345678.control",
                "destination": row["destination"],
            }
        )

    # remove all sft files currently in the ingress staging bucket
    aws_helper.clear_s3_prefix(
        context.data_ingress_stage_bucket, S3_PREFIX_FOR_SFT_OUTPUT, True
    )


@when("we submit them to the '{data_directory}' data directory on the SFT service")
def step_submit_files_to_sft(context, data_directory):
    console_printer.print_info(f"Executing commands on SFT Host")

    commands = ["sudo su"]

    for collection in context.txr_test_collections:
        commands.append(
            f"cd /var/lib/docker/volumes/data-egress/_data/{data_directory}"
        )
        commands.append(f"echo 'test content' >> {collection['file']}")
        commands.append(f"echo 'control test content' >> {collection['controlfile']}")

    console_printer.print_info(
        "Executing the following commands:'{}'".format("\n".join(commands))
    )
    aws_helper.execute_commands_on_ec2_by_tags_and_wait(
        commands, ["dataworks-aws-data-egress"], 5
    )


@then("we verify the collection files are correctly distributed in S3")
def step_verify_stf_content(context):
    # Wait a reasonable time for SFT to transfer
    time.sleep(60)

    missing_collections_in_destination = 0
    collections_that_do_not_reconcile = 0

    for collection in context.txr_test_collections:
        destinations = collection["destination"].split(",")
        for dest in destinations:
            console_printer.print_info(
                f"Checking for collection '{collection['name']}' in destination '{dest}' on S3"
            )
            try:
                output_file_content = (
                    aws_helper.get_s3_object(
                        bucket=context.data_ingress_stage_bucket,
                        key=f"{S3_PREFIX_FOR_SFT_OUTPUT}{dest}/{collection['file']}",
                        s3_client=None,
                    )
                    .decode()
                    .strip()
                )
                control_output_file_content = (
                    aws_helper.get_s3_object(
                        bucket=context.data_ingress_stage_bucket,
                        key=f"{S3_PREFIX_FOR_SFT_OUTPUT}{dest}/{collection['controlfile']}",
                        s3_client=None,
                    )
                    .decode()
                    .strip()
                )
                console_printer.print_info(
                    f"SFT file content is: '{output_file_content}'. sft control file content is : '{control_output_file_content}'"
                )
                if (
                    output_file_content != "test content"
                    or control_output_file_content != "control test content"
                ):
                    collections_that_do_not_reconcile += 1
            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                if error_code == "NoSuchKey":
                    if dest[0] != "!":
                        console_printer.print_error_text(
                            f"Expecting collection '{collection['file']}' in destination '{dest}' on S3: {e}"
                        )
                        missing_collections_in_destination += 1
                else:
                    raise e
            except Exception as e:
                console_printer.print_error_text(
                    f"Could not read object in S3 for collection '{collection['file']}' in destination '{dest}' on S3: {e}"
                )

    assert collections_that_do_not_reconcile == 0
    assert missing_collections_in_destination == 0
