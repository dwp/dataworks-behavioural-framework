import os
import json
from behave import given
from helpers import (
    aws_helper,
    manifest_comparison_helper,
    console_printer,
    invoke_lambda,
)


@given("The manifest generation tables have been created")
def step_impl(context):
    with open(
        os.path.join(context.manifest_templates_local, "drop-table.sql"), "r"
    ) as f:
        base_drop_query = f.read()

    with open(
        os.path.join(context.manifest_templates_local, "create-parquet-table.sql"), "r"
    ) as f:
        base_create_parquet_query = f.read()

    with open(
        os.path.join(
            context.manifest_templates_local, "create-missing-import-table.sql"
        ),
        "r",
    ) as f:
        base_create_missing_import_query = f.read()

    with open(
        os.path.join(
            context.manifest_templates_local, "create-missing-export-table.sql"
        ),
        "r",
    ) as f:
        base_create_missing_export_query = f.read()

    with open(
        os.path.join(context.manifest_templates_local, "create-count-table.sql"), "r"
    ) as f:
        base_create_count_query = f.read()

    tables = [
        [
            context.manifest_missing_imports_table_name,
            base_create_missing_import_query,
            context.manifest_s3_input_parquet_location_missing_import,
        ],
        [
            context.manifest_missing_exports_table_name,
            base_create_missing_export_query,
            context.manifest_s3_input_parquet_location_missing_export,
        ],
        [
            context.manifest_counts_table_name,
            base_create_count_query,
            context.manifest_s3_input_parquet_location_counts,
        ],
        [
            context.manifest_mismatched_timestamps_table_name,
            base_create_parquet_query,
            context.manifest_s3_input_parquet_location_mismatched_timestamps,
        ],
    ]

    for table_details in tables:
        console_printer.print_info(
            f"Dropping table named '{table_details[0]}' if exists"
        )
        drop_query = base_drop_query.replace("[table_name]", table_details[0])
        aws_helper.execute_athena_query(
            context.manifest_s3_output_location_templates, drop_query
        )

        console_printer.print_info(
            f"Generating table named '{table_details[0]}' from S3 location of '{table_details[2]}'"
        )

        s3_location = (
            table_details[2]
            if table_details[2].endswith("/")
            else f"{table_details[2]}/"
        )

        create_query = table_details[1].replace("[table_name]", table_details[0])
        create_query = create_query.replace("[s3_input_location]", s3_location)

        aws_helper.execute_athena_query(
            context.manifest_s3_output_location_templates, create_query
        )


@given("The manifest generation tables have been populated")
def step_impl(context):
    glue_jobs = (
        [context.manifest_etl_glue_job_name]
        if "," not in context.manifest_etl_glue_job_name
        else context.manifest_etl_glue_job_name.split(",")
    )

    for glue_job in glue_jobs:
        if glue_job:
            aws_helper.execute_manifest_glue_job(
                glue_job,
                context.manifest_cut_off_date_start_epoch,
                context.manifest_cut_off_date_end_epoch,
                context.manifest_margin_of_error_epoch,
                context.manifest_snapshot_type,
                context.manifest_import_type,
                context.manifest_s3_input_location_import_prefix,
                context.manifest_s3_input_location_export_prefix,
            )


@when("I generate the manifest comparison queries of type '{query_type}'")
def step_impl(context, query_type):
    context.manifest_queries = []
    for query_file in os.listdir(
        os.path.join(context.manifest_queries_local, query_type)
    ):
        if os.path.splitext(query_file)[1] == ".json":
            with open(
                os.path.join(context.manifest_queries_local, query_type, query_file),
                "r",
            ) as metadata_file:
                metadata = json.loads(metadata_file.read())

            with open(
                os.path.join(
                    context.manifest_queries_local, query_type, metadata["query_file"]
                ),
                "r",
            ) as query_sql_file:
                base_query = query_sql_file.read()

            query = base_query.replace(
                "[parquet_table_name_missing_imports]",
                context.manifest_missing_imports_table_name,
            )
            query = query.replace(
                "[parquet_table_name_missing_exports]",
                context.manifest_missing_exports_table_name,
            )
            query = query.replace(
                "[parquet_table_name_counts]", context.manifest_counts_table_name
            )
            query = query.replace(
                "[parquet_table_name_mismatched]",
                context.manifest_mismatched_timestamps_table_name,
            )
            query = query.replace(
                "[count_of_ids]", str(context.manifest_report_count_of_ids)
            )
            query = query.replace(
                "[specific_id]", "521ee02f-6d75-42da-b02a-560b0bb7cbbc"
            )
            query = query.replace("[specific_timestamp]", "1585055547016")
            query = query.replace(
                "[distinct_default_database_collection_list_full]",
                context.distinct_default_database_collection_list_full,
            )
            query = query.replace(
                "[distinct_default_database_list]",
                context.distinct_default_database_list_full,
            )
            context.manifest_queries.append([metadata, query])


@when(
    "I run the manifest comparison queries of type '{query_type}' and upload the result to S3"
)
def step_impl(context, query_type):
    context.manifest_query_results = []
    context.failed_queries = []
    for query_number in range(1, len(context.manifest_queries) + 1):
        for manifest_query in context.manifest_queries:
            if int(manifest_query[0]["order"]) == query_number:
                if manifest_query[0]["enabled"] and (
                    manifest_query[0]["query_type"] == query_type
                ):
                    console_printer.print_info(
                        f"Running query with name of '{manifest_query[0]['query_name']}' "
                        + f"and description of '{manifest_query[0]['query_description']}' "
                        + f"and order of '{manifest_query[0]['order']}'"
                    )
                    try:
                        aws_helper.clear_session()
                        results_array = [
                            manifest_query[0],
                            aws_helper.execute_athena_query(
                                context.manifest_s3_output_location_queries,
                                manifest_query[1],
                            ),
                        ]
                        context.manifest_query_results.append(results_array)
                    except Exception as ex:
                        console_printer.print_warning_text(
                            f"Error occurred running query named '{manifest_query[0]['query_name']}': '{ex}'"
                        )
                        context.failed_queries.append(manifest_query[0]["query_name"])
                else:
                    console_printer.print_info(
                        f"Not running query with name of '{manifest_query[0]['query_name']}' "
                        + f"because 'enabled' value is set to '{manifest_query[0]['enabled']}'"
                    )

    console_printer.print_info("All queries finished execution")

    console_printer.print_info("Generating test result")
    results_string = manifest_comparison_helper.generate_formatted_results(
        context.manifest_query_results
    )
    console_printer.print_info(f"\n\n\n\n\n{results_string}\n\n\n\n\n")

    results_file_name = f"{context.test_run_name}_results.txt"
    results_file = os.path.join(context.temp_folder, results_file_name)
    with open(results_file, "wt") as open_results_file:
        open_results_file.write(console_printer.strip_formatting(results_string))

    s3_uploaded_location_txt = os.path.join(
        context.manifest_s3_output_prefix_results, results_file_name
    )
    aws_helper.upload_file_to_s3_and_wait_for_consistency(
        results_file,
        context.manifest_s3_bucket,
        context.timeout,
        s3_uploaded_location_txt,
    )

    console_printer.print_bold_text(
        f"Uploaded text results file to S3 bucket with name of '{context.manifest_s3_bucket}' at location '{s3_uploaded_location_txt}'"
    )

    os.remove(results_file)

    console_printer.print_info("Generating json result")
    results_json = manifest_comparison_helper.generate_json_formatted_results(
        context.manifest_query_results, context.test_run_name
    )

    json_file_name = f"{context.test_run_name}_results.json"
    json_file = os.path.join(context.temp_folder, json_file_name)
    with open(json_file, "w") as open_json_file:
        json.dump(results_json, open_json_file, indent=4)

    s3_uploaded_location_json = os.path.join(
        context.manifest_s3_output_prefix_results, json_file_name
    )
    aws_helper.upload_file_to_s3_and_wait_for_consistency(
        json_file,
        context.manifest_s3_bucket,
        context.timeout,
        s3_uploaded_location_json,
    )

    console_printer.print_bold_text(
        f"Uploaded json results file to S3 bucket with name of '{context.manifest_s3_bucket}' at location '{s3_uploaded_location_json}'"
    )

    os.remove(json_file)

    if len(context.failed_queries) > 0:
        raise AssertionError(
            "The following queries failed to execute: "
            + ", ".join(context.failed_queries)
        )
    else:
        console_printer.print_info(f"All queries executed successfully")

    console_printer.print_info(f"Query execution step completed")


@then("The query results match the expected results for queries of type '{query_type}'")
def step_impl(context, query_type):
    if context.manifest_verify_results != "true":
        console_printer.print_info(
            f"Not verifying results as 'context.manifest_verify_results' is set to '{context.manifest_verify_results}' rather than 'true'"
        )
        return

    for manifest_query_result in context.manifest_query_results:
        manifest_query_details = manifest_query_result[0]
        manifest_query_result_file = os.path.join(
            context.manifest_queries_local,
            query_type,
            manifest_query_details["results_file"],
        )

        if os.path.exists(manifest_query_result_file):
            console_printer.print_info(
                f"Verifying results for query with name of '{manifest_query_details['query_name']}' "
                + f"and description of '{manifest_query_details['query_description']}' "
                + f"from results file of '{manifest_query_details['results_file']}'"
            )

            with open(manifest_query_result_file, "r") as results_file_expected:
                results_expected = results_file_expected.read()

            results_actual = manifest_comparison_helper.generate_sql_verification_data(
                manifest_query_result[1]
            )

            results_expected_array = results_expected.splitlines()
            console_printer.print_info(f"Expected results: '{results_expected_array}'")

            results_actual_array = results_actual.splitlines()
            console_printer.print_info(f"Actual results: '{results_actual_array}'")

            console_printer.print_info("Verifying results")

            console_printer.print_info("Asserting results length")
            assert len(results_expected_array) == len(results_actual_array)
            console_printer.print_info("Asserting result rows")
            assert all(
                elem in results_expected.splitlines()
                for elem in results_actual.splitlines()
            )
        else:
            console_printer.print_info(
                f"Not verifying results for query with name of '{manifest_query_details['query_name']}' "
                + f"and description of '{manifest_query_details['query_description']}' "
                + f"as no results file exists at '{manifest_query_details['results_file']}'"
            )


@then("The query results are printed")
def step_impl(context):
    for manifest_query_result in context.manifest_query_results:
        manifest_query_details = manifest_query_result[0]

        console_printer.print_info(
            f"Printing results for query with name of '{manifest_query_details['query_name']}' "
            + f"and description of '{manifest_query_details['query_description']}'"
        )

        results_actual = manifest_comparison_helper.generate_sql_verification_data(
            manifest_query_result[1]
        )

        results_actual_array = results_actual.splitlines()
        console_printer.print_info(f"Actual results: '{results_actual_array}'")


@when("I start the kafka reconciliation")
def step_impl(context):
    payload = {
        "detail": {
            "jobName": f"{context.test_run_name}",
            "jobQueue": "dataworks-behavioural-framework",
            "status": "SUCCEEDED",
            "ignoreBatchChecks": "true",
        }
    }

    payload_json = json.dumps(payload)
    console_printer.print_info(f"Glue launcher lambda payload is: '{payload_json}'")
    invoke_lambda.invoke_glue_launcher_lambda(payload_json)

    console_printer.print_info(f"Kafka reconciliation started via Glue launcher lambda")
