from behave import given, when, then
import os
import json
from datetime import datetime, timedelta
from helpers import (
    emr_step_generator,
    aws_helper,
    invoke_lambda,
    console_printer,
    file_helper,
    kickstart_adg_helper,
    historic_data_load_generator,
)

S3_KEY_KICSKTART_TEST = "kickstart-e2e-tests"
COMPLETED_STATUS = "COMPLETED"
CLUSTER_ARN = "ClusterArn"
AUDIT_TABLE_HASH_KEY = "Correlation_Id"
AUDIT_TABLE_RANGE_KEY = "DataProduct"
DYNAMO_DB_TABLE_NAME = "data_pipeline_metadata"

@given(
    "The template file '{template_name}' as an input, generate '{record_count}' records per table for '{module_name}'"
)
def step_impl(context, template_name, record_count, module_name):

    console_printer.print_info(
        f"Extracting the file properties from {template_name} for module {module_name}"
    )
    context.kickstart_schema_config = kickstart_adg_helper.get_schema_config(
        context.fixture_path_local, template_name
    )[module_name]

    console_printer.print_info(
        f"generating the input datasets locally with {record_count} records per file for given config \n"
        f"{json.dumps(context.kickstart_schema_config)}"
    )
    list_of_local_files = kickstart_adg_helper.generate_data(
        module_name, record_count, context.kickstart_schema_config, context.temp_folder
    )

    console_printer.print_info(
        f"Adding the list of files generated for current e2e test run into context for validation steps. The list of files are \n"
        f"{list_of_local_files}"
    )
    context.kickstart_current_run_input_files = list_of_local_files

@then(
    "upload the local files to s3 bucket in '{data_encryption}' format"
)
def step_impl(context, data_encryption):

    for file in context.kickstart_current_run_input_files:
        if data_encryption.lower() == "unencrypted":
            console_printer.print_info(
                f"Data will be uploaded in {data_encryption} format to s3 bucket"
            )
            console_printer.print_info(f"The file name is {file}")
            file_name = os.path.basename(file)
            input_file = file_helper.get_contents_of_file(file, False)
            inputs_s3_key = os.path.join(S3_KEY_KICSKTART_TEST, file_name)
            console_printer.print_info(
                f"Uploading the local file {file} with basename as {file_name} into s3 bucket {context.published_bucket} using key name as {inputs_s3_key}"
            )
            aws_helper.put_object_in_s3(
                input_file, context.published_bucket, inputs_s3_key
            )
        elif data_encryption.lower() == "encrypted":
            console_printer.print_info(
                f"Data will be uploaded in {data_encryption} format to s3 bucket"
            )
            console_printer.print_info(f"The input file name is {file}")

            file_name = os.path.basename(file)
            encrypted_key = context.encryption_encrypted_key
            master_key = context.encryption_master_key_id
            plaintext_key = context.encryption_plaintext_key
            [
                file_iv_int,
                file_iv_whole,
            ] = historic_data_load_generator.generate_initialisation_vector()

            console_printer.print_info(f"Extracting the raw data from local directory")
            data = file_helper.get_contents_of_file(file, False).encode("utf-8")

            console_printer.print_info(f"Applying encryption to the raw data")
            input_data = historic_data_load_generator.encrypt(
                file_iv_whole, plaintext_key, data
            )
            inputs_s3_key = os.path.join(S3_KEY_KICSKTART_TEST, file_name+'.enc')

            all_metadata=json.loads(historic_data_load_generator.generate_encryption_metadata_for_metadata_file(
                encrypted_key, master_key, plaintext_key, file_iv_int
            ))

            console_printer.print_info(f"Metadata of for encrypted file is {json.dumps(all_metadata)}")

            metadata = {
                "iv" : all_metadata["initialisationVector"],
                "ciphertext" : all_metadata["encryptedencryptionkey"],
                "datakeyencryptionkeyid" : all_metadata["keyencryptionkeyid"]
            }
            console_printer.print_info(
                f"Uploading the local file {file} with basename as {file_name} into s3 bucket {context.published_bucket} using key name as {inputs_s3_key} and along with metadata"
            )

            aws_helper.put_object_in_s3_with_metadata(
                input_data, context.published_bucket, inputs_s3_key, metadata=metadata
            )
