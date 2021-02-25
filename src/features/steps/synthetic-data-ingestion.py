from behave import given, when, then
from helpers import aws_helper, historic_data_load_generator, console_printer
import os
import re


@given("synthetic raw data in datasets bucket")
def step_impl(context):
    encrypted_key = context.encryption_encrypted_key
    master_key = context.encryption_master_key_id
    plaintext_key = context.encryption_plaintext_key
    common_pattern = "[\w-]+\.[\w-]+\.[0-9]+\.json"
    gzip_pattern = f"{common_pattern}\.gz"
    datasets_bucket = context.aws_datasets_bucket
    synthetic_rawdata_prefix = context.synthetic_rawdata_prefix

    directory = context.temp_folder
    if not os.path.exists(directory):
        os.makedirs(directory)

    s3_client = aws_helper.get_client(service_name="s3")

    for file in aws_helper.retrieve_files_from_s3_with_bucket_and_path(
        s3_client, datasets_bucket, synthetic_rawdata_prefix
    ):
        key = file["Key"]
        matched_key = re.search(gzip_pattern, key, flags=0)
        if matched_key is not None:
            input_file_name = matched_key.group() + ".enc"
            metadata_file_name = re.findall(common_pattern, key)[0] + ".encryption.json"
            input_file_path = os.path.join(directory, input_file_name)
            metadata_file_path = os.path.join(directory, metadata_file_name)
            [
                file_iv_int,
                file_iv_whole,
            ] = historic_data_load_generator.generate_initialisation_vector()
            data = aws_helper.get_s3_object(s3_client, datasets_bucket, key)
            input_data = historic_data_load_generator.encrypt(
                file_iv_whole, plaintext_key, data
            )
            meta_data = historic_data_load_generator.generate_encryption_metadata_for_metadata_file(
                encrypted_key, master_key, plaintext_key, file_iv_int
            )
            with open(input_file_path, "wb") as data:
                data.write(input_data)
            historic_data_load_generator.generate_encryption_input_metadata_file(
                metadata_file_path, meta_data
            )
            console_printer.print_info(
                "Files in %r: %s" % (directory, os.listdir(directory))
            )

        else:
            error_msg = f"{key} not in expected pattern {gzip_pattern}"
            console_printer.print_info(error_msg)

            raise AssertionError(error_msg)


@when("uploaded encrypted data and metadata files to ingest bucket")
def step_impl(context):
    s3_ingest_bucket = context.s3_ingest_bucket
    synthetic_encdata_prefix = context.synthetic_encdata_prefix
    synthetic_encdata_comp_prefix = (
        f"{context.ucfs_historic_data_prefix}/{synthetic_encdata_prefix}"
    )
    directory = context.temp_folder
    if not os.path.exists(directory):
        os.makedirs(directory)
    s3_client1 = aws_helper.get_client(service_name="s3")
    for root, dirs, files in os.walk(directory):
        for file in files:
            aws_helper.upload_file_to_s3_and_wait_for_consistency(
                os.path.join(root, file),
                s3_ingest_bucket,
                context.timeout,
                os.path.join(synthetic_encdata_comp_prefix, file),
                s3_client1,
            )
    context.test_run_name = synthetic_encdata_prefix
