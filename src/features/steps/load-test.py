import os
import uuid
from behave import given, when, then
from helpers import (
    aws_helper,
    template_helper,
    historic_data_load_generator,
    file_helper,
    console_printer,
)


@given(
    "{file_count} files are generated and uploaded with {record_count} records and key method of '{method}'"
)
def step_impl(context, file_count, record_count, method):
    generated_output_folder = file_helper.generate_edited_files_folder(
        context.temp_folder, context.test_run_name
    )
    input_folder = os.path.join(generated_output_folder, "input")
    input_template = os.path.join(
        context.fixture_path_local, "historic_data_valid", "input_template.json"
    )

    for topic in context.topics_for_test:
        if os.path.exists(input_folder):
            file_helper.clear_and_delete_directory(input_folder)

        os.makedirs(input_folder)

        context.uploaded_id = uuid.uuid4() if method == "static" else None
        method_qualified = "single" if method == "static" else method

        historic_data_prefix_base = os.path.join(
            context.ucfs_historic_data_prefix, context.test_run_name
        )

        historic_data_load_generator.generate_historic_load_data(
            context.s3_ingest_bucket,
            historic_data_prefix_base,
            method_qualified,
            int(file_count),
            int(record_count),
            topic["topic"],
            input_template,
            context.encryption_encrypted_key,
            context.encryption_plaintext_key,
            context.encryption_master_key_id,
            input_folder,
            int(context.load_tests_data_max_worker_count),
            context.uploaded_id,
        )


@when(
    "UCFS upload desired number of files with desired number of records per collection using load test key method"
)
def step_impl(context):
    context.execute_steps(
        f"given UCFS upload '{context.load_tests_file_count}' files with input filename of 'input_template.json' "
        + "and output filename of 'output_template.json' "
        + "and snapshot record filename of 'None' "
        + f"with '{context.load_tests_record_count}' records and key method of "
        + f"'{context.load_test_key_method}' and type of 'output'"
    )


@given(
    "UCFS send a high message volume of type '{message_type}' with input file of '{input_file_name}' and output file of '{output_file_name}'"
)
def step_impl(context, message_type, input_file_name, output_file_name):
    record_count = context.kafka_record_count if context.kafka_record_count else "100"
    context.execute_steps(
        f"given UCFS send '{record_count}' message of type '{message_type}' with input file of "
        + f"'{input_file_name}', output file of '{output_file_name}', dlq file of 'None', "
        + f"snapshot record file of 'None', "
        + f"encryption setting of 'true' and wait setting of 'false' with key method of 'static'"
    )


@when(
    "UCFS upload desired number of input only files with desired number of records per collection using load test key method"
)
def step_impl(context):
    context.execute_steps(
        f"given {context.load_tests_file_count} files are generated and uploaded with {context.load_tests_record_count} records and key method of '{context.load_test_key_method}'"
    )


@given("The S3 HDI files are cleared")
def step_impl(context):
    console_printer.print_info("Cleared S3 files via fixture")


@given("HBase is cleared")
def step_impl(context):
    console_printer.print_info("Cleared HBase via fixture")


@given("The S3 snapshots are cleared")
def step_impl(context):
    console_printer.print_info("Cleared S3 snapshots via fixture")
