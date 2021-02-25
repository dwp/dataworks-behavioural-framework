import os
import uuid
from behave import given, when
from helpers import aws_helper, historic_data_generator, template_helper, file_helper
from helpers import (
    message_helper,
    file_comparer,
    console_printer,
    snapshot_data_generator,
)


@given("Skip existing records start time is set to the time from file '{}'")
def step_impl(context, skip_existing):
    all_prefixes = template_helper.get_historic_data_importer_prefixes(
        context.test_run_name, context.historic_importer_use_one_message_per_path
    )


@given("Skip existing records end time is set to the time from file '{}'")
def step_impl(context, skip_existing):
    all_prefixes = template_helper.get_historic_data_importer_prefixes(
        context.test_run_name, context.historic_importer_use_one_message_per_path
    )


@when(
    "The import process is performed with skip existing records setting of '{skip_existing}'"
)
def step_impl(context, skip_existing):
    all_prefixes = template_helper.get_historic_data_importer_prefixes(
        context.test_run_name, context.historic_importer_use_one_message_per_path
    )
    skip_existing_value = skip_existing.lower() == "true"
    skip_earlier_than = (
        None
        if not context.historic_data_ingestion_skip_earlier_than_override
        else context.historic_data_ingestion_skip_earlier_than_override
    )
    skip_later_than = (
        None
        if not context.historic_data_ingestion_skip_later_than_override
        else context.historic_data_ingestion_skip_later_than_override
    )
    correlation_id = (
        context.test_run_name
        if not context.historic_importer_correlation_id_override
        else context.historic_importer_correlation_id_override
    )

    for prefix in all_prefixes:
        message_helper.send_start_import_message(
            context.aws_sqs_queue_historic_data_importer,
            prefix,
            skip_earlier_than,
            skip_later_than,
            context.test_run_name,
            run_import=True,
            generate_manifest=False,
            skip_existing_records=skip_existing_value,
            correlation_id=correlation_id,
        )


@given(
    "The import process is performed and a manifest is generated with skip existing records setting of '{skip_existing}'"
)
@when(
    "The import process is performed and a manifest is generated with skip existing records setting of '{skip_existing}'"
)
def step_impl(context, skip_existing):
    all_prefixes = template_helper.get_historic_data_importer_prefixes(
        context.test_run_name, context.historic_importer_use_one_message_per_path
    )
    skip_existing_value = skip_existing.lower() == "true"
    skip_earlier_than = (
        None
        if not context.historic_data_ingestion_skip_earlier_than_override
        else context.historic_data_ingestion_skip_earlier_than_override
    )
    skip_later_than = (
        None
        if not context.historic_data_ingestion_skip_later_than_override
        else context.historic_data_ingestion_skip_later_than_override
    )
    correlation_id = (
        context.test_run_name
        if not context.historic_importer_correlation_id_override
        else context.historic_importer_correlation_id_override
    )

    for prefix in all_prefixes:
        message_helper.send_start_import_message(
            context.aws_sqs_queue_historic_data_importer,
            prefix,
            skip_earlier_than,
            skip_later_than,
            context.test_run_name,
            run_import=True,
            generate_manifest=True,
            skip_existing_records=skip_existing_value,
            correlation_id=correlation_id,
        )


@given(
    "UCFS upload '{file_count}' files for each of the given template files with '{record_count}' records and key method of '{method}' and type of '{file_type}'"
)
def step_impl(context, file_count, record_count, method, file_type):
    for row in context.table:
        context.execute_steps(
            f"given UCFS upload '{file_count}' files with input filename of '{row['input-file-name-import']}' "
            + f"and output filename of '{row['output-file-name-import']}' "
            + f"and snapshot record filename of '{row['snapshot-record-file-name-import']}' "
            + f"with '{record_count}' records and key method of "
            + f"'{method}' and type of '{file_type}'"
        )


@given(
    "UCFS upload files for each of the given template files with type of '{file_type}'"
)
def step_impl(context, file_type):
    file_count = os.getenv("DATA_GENERATION_FILE_COUNT")
    record_count = os.getenv("DATA_GENERATION_RECORD_COUNT")
    key_method = os.getenv("DATA_GENERATION_METHOD")

    for row in context.table:
        context.execute_steps(
            f"given UCFS upload '{file_count}' files with input filename of '{row['input-file-name-import']}' "
            + f"and output filename of '{row['output-file-name-import']}' "
            + f"and snapshot record filename of '{row['snapshot-record-file-name-import']}' "
            + f"with '{record_count}' records and key method of "
            + f"'{key_method}' and type of '{file_type}'"
        )


@given(
    "UCFS upload '{file_count}' files with input filename of '{input_file_name}' and output filename of '{output_file_name}' and snapshot record filename of '{snapshot_record_file_name}' with '{record_count}' records and key method of '{method}' and type of '{file_type}'"
)
def step_impl(
    context,
    file_count,
    input_file_name,
    output_file_name,
    snapshot_record_file_name,
    record_count,
    method,
    file_type,
):
    context.historic_data_generations_count_per_test += 1

    generated_output_folder = file_helper.generate_edited_files_folder(
        context.temp_folder, context.test_run_name
    )
    input_folder = os.path.join(generated_output_folder, "input")

    input_template = os.path.join(
        context.fixture_path_local, "historic_data_valid", input_file_name
    )
    output_template = os.path.join(
        context.fixture_path_local, "historic_data_valid", output_file_name
    )

    context.importer_output_folder = os.path.join(generated_output_folder, "output")
    if os.path.exists(context.importer_output_folder):
        file_helper.clear_and_delete_directory(context.importer_output_folder)
    os.makedirs(context.importer_output_folder)

    context.encryption_iv_dbobject_tuple = (
        historic_data_generator.generate_initialisation_vector()
    )
    context.encryption_iv_file_tuple = (
        historic_data_generator.generate_initialisation_vector()
    )

    for topic in context.topics_for_test:
        output_qualified = (
            None
            if file_type == "input"
            else os.path.join(context.importer_output_folder, topic["topic"])
        )

        if not os.path.exists(generated_output_folder):
            os.makedirs(generated_output_folder)

        context.uploaded_id = uuid.uuid4() if method == "static" else None
        method_qualified = "single" if method == "static" else method
        snapshot_record_file_name_qualified = (
            snapshot_record_file_name if snapshot_record_file_name != "None" else None
        )

        historic_data_generator.generate_historic_data(
            context.test_run_name,
            method_qualified,
            int(file_count),
            int(record_count),
            topic["topic"],
            input_template,
            output_template,
            context.fixture_path_local,
            snapshot_record_file_name_qualified,
            context.encryption_encrypted_key,
            context.encryption_plaintext_key,
            context.encryption_master_key_id,
            input_folder,
            output_qualified,
            context.encryption_iv_dbobject_tuple,
            context.encryption_iv_file_tuple,
            context.snapshot_files_hbase_records_temp_folder,
            context.uploaded_id,
            context.historic_data_generations_count_per_test,
        )

        historic_data_prefix_base = os.path.join(
            context.ucfs_historic_data_prefix, context.test_run_name
        )

        console_printer.print_info(
            f"Uploading generated files for collection {topic['topic']} to "
            + f"{historic_data_prefix_base} in bucket {context.s3_ingest_bucket}"
        )

        for result in aws_helper.upload_file_to_s3_and_wait_for_consistency_threaded(
            input_folder,
            context.s3_ingest_bucket,
            context.timeout,
            historic_data_prefix_base,
        ):
            console_printer.print_info(f"Uploaded file to s3 with key of {result}")

        aws_helper.clear_session()


@when(
    "UCFS upload {file_count} files with {record_count} records per collection with the uploaded id"
)
def step_impl(context, file_count, record_count):
    context.historic_data_generations_count_per_test += 1

    generated_output_folder = file_helper.generate_edited_files_folder(
        context.temp_folder, context.test_run_name
    )
    input_folder = os.path.join(generated_output_folder, "input")
    input_template = os.path.join(
        context.fixture_path_local, "historic_data_valid", "input_template.json"
    )
    output_template = os.path.join(
        context.fixture_path_local, "historic_data_valid", "output_template.json"
    )

    context.importer_output_folder = os.path.join(generated_output_folder, "output")
    if os.path.exists(context.importer_output_folder):
        file_helper.clear_and_delete_directory(context.importer_output_folder)
    os.makedirs(context.importer_output_folder)

    context.encryption_iv_dbobject_tuple = (
        historic_data_generator.generate_initialisation_vector()
    )
    context.encryption_iv_file_tuple = (
        historic_data_generator.generate_initialisation_vector()
    )

    for export_topic in context.topics_for_test:
        if os.path.exists(input_folder):
            file_helper.clear_and_delete_directory(input_folder)
        os.makedirs(input_folder)

        historic_data_generator.generate_historic_data(
            context.test_run_name,
            "single",
            int(file_count),
            int(record_count),
            export_topic["topic"],
            input_template,
            output_template,
            context.fixture_path_local,
            None,
            context.encryption_encrypted_key,
            context.encryption_plaintext_key,
            context.encryption_master_key_id,
            input_folder,
            os.path.join(context.importer_output_folder, export_topic["topic"]),
            context.encryption_iv_dbobject_tuple,
            context.encryption_iv_file_tuple,
            None,
            context.uploaded_id,
            context.historic_data_generations_count_per_test,
        )

        historic_data_prefix_base = os.path.join(
            context.ucfs_historic_data_prefix, context.test_run_name
        )

        console_printer.print_info(
            f"Uploading generated files for collection {export_topic['topic']} to "
            + f"{historic_data_prefix_base} in bucket {context.s3_ingest_bucket}"
        )

        for result in aws_helper.upload_file_to_s3_and_wait_for_consistency_threaded(
            input_folder,
            context.s3_ingest_bucket,
            context.timeout,
            historic_data_prefix_base,
        ):
            console_printer.print_info(f"Uploaded file to s3 with key of {result}")
