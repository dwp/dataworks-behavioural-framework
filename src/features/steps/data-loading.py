import os
import uuid
from datetime import datetime, timedelta
from behave import given, when
from helpers import (
    emr_step_generator,
    data_load_helper,
    template_helper,
    corporate_data_generator,
    aws_helper,
)


@given("We generate corporate data for each of the given template files")
def step_impl(context):
    record_count = os.getenv("DATA_GENERATION_RECORD_COUNT")
    key_method = os.getenv("DATA_GENERATION_METHOD")

    for row in context.table:
        context.execute_steps(
            f"given We generate corporate data of '{record_count}' messages with input file of "
            + f"'{row['input-file-name-corporate']}' and output file of '{row['output-file-name-corporate']}' "
            + f"with key method of '{key_method}' and days offset of 'None'"
        )


@given(
    "We generate corporate data of '{record_count}' records with the given template files with key method of '{key_method}' and days offset of '{days_offset}'"
)
@given(
    "We generate corporate data of '{record_count}' record with the given template files with key method of '{key_method}' and days offset of '{days_offset}'"
)
def step_impl(context, record_count, key_method, days_offset):
    for row in context.table:
        context.execute_steps(
            f"given We generate corporate data of '{record_count}' messages with input file of "
            + f"'{row['input-file-name-corporate']}' and output file of '{row['output-file-name-corporate']}' "
            + f"with key method of '{key_method}' and days offset of '{days_offset}'"
        )


@given(
    "We generate corporate data of '{record_count}' messages with input file of '{input_file_name}' and output file of '{output_file_name}' with key method of '{key_method}' and days offset of '{days_offset}'"
)
@given(
    "We generate corporate data of '{record_count}' message with input file of '{input_file_name}' and output file of '{output_file_name}' with key method of '{key_method}' and days offset of '{days_offset}'"
)
def step_impl(
    context,
    record_count,
    input_file_name,
    output_file_name,
    key_method,
    days_offset,
):
    context.uploaded_id = uuid.uuid4()

    output_template = None if output_file_name == "None" else output_file_name

    for topic in context.topics_for_test:
        key = None
        if key_method.lower() == "static":
            key = context.uploaded_id
        elif key_method.lower() == "topic":
            key = uuid.uuid4()

        topic_name = template_helper.get_topic_name(topic["topic"])
        timestamp_override = (
            datetime.now() + timedelta(days=int(days_offset))
            if days_offset and days_offset.lower() != "none"
            else None
        )

        corporate_data_generator.generate_corporate_data_files(
            context.test_run_name,
            context.corporate_storage_s3_bucket_id,
            input_file_name,
            output_template,
            key,
            os.path.join(context.temp_folder, topic_name),
            context.fixture_path_local,
            context.cdl_data_load_s3_base_prefix_tests,
            record_count,
            topic["topic"],
            context.timeout,
            timestamp_override,
        )


@when("The historic data is loaded in to HBase with arguments of '{argument_type}'")
def step_impl(context, argument_type):
    arguments = context.historic_data_load_run_script_arguments

    if argument_type.lower() != "default":
        topics = [topic["topic"] for topic in context.topics_for_test]

        arguments = data_load_helper.generate_arguments_for_historic_data_load(
            context.test_run_name,
            ",".join(topics),
            context.ucfs_historic_data_prefix,
            context.test_run_name,
            None,
            context.historic_data_ingestion_skip_earlier_than_override,
            context.historic_data_ingestion_skip_later_than_override,
        )

    context.data_load_step_id = emr_step_generator.generate_script_step(
        context.ingest_hbase_emr_cluster_id,
        "/opt/emr/run_hdl.sh",
        "hdl",
        arguments,
    )


@when("The corporate data is loaded in to HBase with default settings")
def step_impl(context):
    context.execute_steps(
        f"when The corporate data is loaded in to HBase for the 'default' metadata store with arguments of 'default', start date days offset of 'None', end date days offset of 'None', partition count of 'None' and prefix per execution setting of 'false'"
    )


@when(
    "The corporate data is loaded in to HBase for the '{metadata_store_table_name}' metadata store with arguments of '{argument_type}', start date days offset of '{start_days_offset}', end date days offset of '{end_days_offset}', partition count of '{partition_count}' and prefix per execution setting of '{prefix_per_execution}'"
)
def step_impl(
    context,
    metadata_store_table_name,
    argument_type,
    start_days_offset,
    end_days_offset,
    partition_count,
    prefix_per_execution,
):
    arguments = context.corporate_data_load_run_script_arguments

    if argument_type.lower() != "default":
        start_date = (
            (datetime.now() + timedelta(days=int(start_days_offset))).strftime(
                "%Y-%m-%d"
            )
            if start_days_offset and start_days_offset.lower() != "none"
            else None
        )
        end_date = (
            (datetime.now() + timedelta(days=int(end_days_offset))).strftime("%Y-%m-%d")
            if end_days_offset and end_days_offset.lower() != "none"
            else None
        )
        partition_count = (
            partition_count
            if partition_count and partition_count.lower() != "none"
            else None
        )
        topics = [topic["topic"] for topic in context.topics_for_test]

        arguments = data_load_helper.generate_arguments_for_corporate_data_load(
            context.test_run_name,
            ",".join(topics),
            context.cdl_data_load_s3_base_prefix_tests,
            metadata_store_table_name,
            None,
            context.cdl_file_pattern_ucfs,
            start_date,
            end_date,
            partition_count,
            prefix_per_execution,
        )

    context.data_load_step_id = emr_step_generator.generate_script_step(
        context.ingest_hbase_emr_cluster_id,
        "/opt/emr/run_cdl.sh",
        "cdl",
        arguments,
    )


@then(
    "The data load is completed successfully with timeout setting of '{timeout_setting}'"
)
def step_impl(context, timeout_setting):
    if timeout_setting == "true":
        timeout = context.timeout
    elif timeout_setting == "long":
        timeout = context.timeout * 5
    else:
        timeout = None

    execution_state = aws_helper.poll_emr_cluster_step_status(
        context.data_load_step_id, context.ingest_hbase_emr_cluster_id, timeout
    )

    if execution_state != "COMPLETED":
        raise AssertionError(
            f"Data load did not complete successfully with final status of '{execution_state}'"
        )


@given(
    "The create tables process is started on HBase with arguments of '{argument_type}'"
)
def step_impl(context, argument_type):
    topics = context.data_load_topics

    if argument_type.lower() != "default":
        topics = ",".join([topic["topic"] for topic in context.topics_for_test])

    context.create_hbase_tables_step_id = emr_step_generator.generate_script_step(
        context.ingest_hbase_emr_cluster_id,
        "/opt/emr/create_hbase_tables.sh",
        "create hbase tables",
        f"{topics} true",
    )


@given("The create tables step is completed successfully")
def step_impl(context):
    execution_state = aws_helper.poll_emr_cluster_step_status(
        context.create_hbase_tables_step_id, context.ingest_hbase_emr_cluster_id, 1200
    )

    if execution_state != "COMPLETED":
        raise AssertionError(
            f"Create tables did not complete successfully with final status of '{execution_state}'"
        )
