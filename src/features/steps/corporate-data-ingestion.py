import json
import uuid
import os
from behave import given, when, then
from helpers import (
    snapshot_data_generator,
    historic_data_load_generator,
    aws_helper,
    invoke_lambda,
    console_printer,
    emr_step_generator,
    file_helper,
    export_status_helper,
    data_pipeline_metadata_helper,
    corporate_data_generator,
    data_load_helper,
)
from datetime import datetime

CLUSTER_ARN = "ClusterArn"
COMPLETED_STATUS = "COMPLETED"
CORRELATION_ID = "correlation_id"
S3_PREFIX = "s3_prefix"
EXPORT_DATE = "export_date"
SNAPSHOT_TYPE = "snapshot_type"


@given("s3 '{location_type}' prefix replaced by unauthorised location")
def step_(context, location_type):
    if location_type == "source":
        context.s3_generated_records_input_prefix = "unauthorised_location/e2e"
    elif location_type == "destination":
        context.s3_output_prefix = "unauthorised_location/e2e"
    else:
        raise AttributeError(
            "Parameter 'location_type' must be one of two values: 'source' or 'destination'"
        )


@given(
    "we generate '{record_count}' records with the input template '{input_template}' and store them in the Corporate Storage S3 bucket"
)
def step_(context, record_count, input_template):
    context.type_of_record = "valid_record"
    context.s3_generated_records_input_prefix = os.path.join(
        context.s3_generated_records_input_prefix, context.type_of_record
    )
    context.record_count = int(record_count)

    aws_helper.clear_s3_prefix(
        s3_bucket=context.corporate_storage_s3_bucket_id,
        path=context.s3_generated_records_input_prefix,
        delete_prefix=True,
    )

    for record_number in range(1, int(record_count) + 1):

        key = uuid.uuid4()

        corporate_data_generator.generate_corporate_data_files(
            test_run_name=context.test_run_name,
            s3_input_bucket=context.corporate_storage_s3_bucket_id,
            input_template_name=input_template,
            output_template_name=input_template.replace("input", "output"),
            new_uuid=key,
            local_files_temp_folder=os.path.join(
                context.temp_folder, "e2e", "businessAudit", context.type_of_record
            ),
            fixture_files_root=context.fixture_path_local,
            s3_output_prefix="",
            record_count=record_count,
            topic_name=".".join(["data", "businessAudit"]),
            seconds_timeout=context.timeout,
            timestamp_override=datetime.now(),
            s3_output_prefix_override=context.s3_generated_records_input_prefix,
        )


@given(
    "we generate a corrupted archive and store it in the Corporate Storage S3 bucket"
)
def step_(context):
    filename = f"corrupted_archive.jsonl.gz"
    aws_helper.upload_file_to_s3_and_wait_for_consistency(
        file_location=os.path.join(
            context.fixture_path_local, "corporate_data", filename
        ),
        s3_bucket=context.corporate_storage_s3_bucket_id,
        seconds_timeout=context.timeout,
        s3_key=os.path.join(context.s3_generated_records_input_prefix, filename),
    )
    context.record_count += 1


@when("corporate-data-ingestion EMR step triggered")
def step_(context):
    context.s3_output_prefix = os.path.join(
        context.s3_output_prefix, context.type_of_record
    )
    context.correlation_id = f"corporate_data_ingestion_{uuid.uuid4()}"
    step_id = emr_step_generator.generate_spark_step(
        emr_cluster_id=context.corporate_data_ingestion_cluster_id,
        script_location="/opt/emr/steps/corporate-data-ingestion.py",
        step_type=f"""businessAudit ingestion testing with {context.type_of_record}""",
        command_line_arguments=f"""--correlation_id {context.correlation_id} """
        f"""--source_s3_prefix {context.s3_generated_records_input_prefix} """
        f"""--destination_s3_prefix {context.s3_output_prefix}""",
    )
    context.execution_state = aws_helper.poll_emr_cluster_step_status(
        step_id, context.corporate_data_ingestion_cluster_id
    )


@then("confirm that the EMR step status is '{status}'")
def step_(context, status):
    if context.execution_state != status:
        raise AssertionError(
            f"""'businessAudit ingestion testing with {context.type_of_record}' step failed with final status of '{context.execution_state}'"""
        )


@then(
    "confirm that the number of generated records equals the number of ingested records"
)
def step_(context):
    result = aws_helper.get_s3_object(
        s3_client=None,
        bucket=context.published_bucket,
        key=f"corporate_data_ingestion/audit_logs_transition/results/{context.correlation_id}/result.json",
    )
    if result:
        result_json = json.loads(result)
        assert int(result_json["record_ingested_count"]) == int(context.record_count)
    else:
        raise AssertionError(f"""Unable to read cluster result file""")
