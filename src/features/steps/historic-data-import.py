from behave import given, then
from helpers import aws_helper, console_printer, template_helper, message_helper


@given("The skip earlier time override is set as '{override_date_time}'")
def step_impl(context, override_date_time):
    context.historic_data_ingestion_skip_earlier_than_override = override_date_time


@given("The skip later time override is set as '{override_date_time}'")
def step_impl(context, override_date_time):
    context.historic_data_ingestion_skip_later_than_override = override_date_time


@given("The UC data import prefixes are configured")
def step_impl(context):
    assert context.mongo_data_load_prefixes_comma_delimited


@then(
    "The UC historic data is imported from each prefix with setting of '{setting}' and skip existing records setting of '{skip_existing}'"
)
def step_impl(context, setting, skip_existing):
    run_import_value = (
        True if setting == "import" or setting == "import and manifest" else False
    )
    run_manifest_value = (
        True if setting == "manifest" or setting == "import and manifest" else False
    )
    skip_existing_value = (
        context.historic_data_ingestion_skip_existing_records_override is not None
        and context.historic_data_ingestion_skip_existing_records_override.lower()
        == "true"
    )
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

    console_printer.print_info(
        "UC historic data import prefixes: "
        + f"{context.mongo_data_load_prefixes_comma_delimited}"
    )

    all_prefixes = template_helper.get_historic_data_importer_prefixes(
        context.mongo_data_load_prefixes_comma_delimited,
        context.historic_importer_use_one_message_per_path,
    )

    correlation_id = (
        context.test_run_name
        if not context.historic_importer_correlation_id_override
        else context.historic_importer_correlation_id_override
    )

    for prefix in all_prefixes:
        console_printer.print_info(
            f"Sending work to historic importer for {prefix} in {context.s3_ingest_bucket}"
        )

        message_helper.send_start_import_message(
            context.aws_sqs_queue_historic_data_importer,
            prefix,
            skip_earlier_than,
            skip_later_than,
            context.test_run_name,
            run_import=run_import_value,
            generate_manifest=run_manifest_value,
            skip_existing_records=skip_existing_value,
            correlation_id=correlation_id,
        )
