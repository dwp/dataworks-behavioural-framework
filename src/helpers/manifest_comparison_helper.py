import os
import json
import datetime
from helpers import console_printer, aws_helper


def generate_formatted_results(results_array):
    """Generates the results text for the given set of results.

    Keyword arguments:
    results_array -- an array of results, where each item is an array of [manifest_details_array, results_json]
    """
    results_string = console_printer.generate_header("QUERY RESULTS:\n\n")
    for results_instance in results_array:
        results_string += generate_formatted_result(results_instance)

    return results_string


def generate_formatted_result(results_instance):
    """Generates the results text for a given result.

    Keyword arguments:
    results_instance -- results as an array of [manifest_details_array, results_json]
    """
    manifest_details_array = results_instance[0]
    console_printer.print_info(
        f"Formatting results for query with name of: '{manifest_details_array['query_name']}'"
    )
    result_string = f"""{console_printer.generate_bold_text('Name')}: {manifest_details_array['query_name']}
{console_printer.generate_bold_text('Description')}: {manifest_details_array['query_description']}
{generate_formatted_sql_results(results_instance[1], manifest_details_array['show_column_names'])}
"""

    return result_string


def generate_formatted_sql_results(sql_results_instance, show_columns):
    """Generates the results text for a given result sql.

    Keyword arguments:
    sql_results_instance -- the sql results returned from athena
    show_columns -- boolean for whether to show column names or not in the results
    """
    results_string = f"{console_printer.generate_bold_text('Results')}:\n"
    all_rows = sql_results_instance["ResultSet"]["Rows"]
    column_array = all_rows[0] if show_columns else None
    for row_index in range(1, len(all_rows)):
        results_string += (
            f"\t-- {generate_formatted_sql_row(all_rows[row_index], column_array)}\n"
        )

    return results_string


def generate_formatted_sql_row(row, column_row):
    """Generates the results text for a given result sql.

    Keyword arguments:
    row -- the row results from athena
    column_row -- the first row from athena result set (contains columns, not data) - if nothing, no columns shown
    """
    row_string = ""
    row_data = row["Data"]

    for column_index in range(0, len(row_data)):
        if column_row is not None:
            row_string += f"{console_printer.generate_italic_text(column_row['Data'][column_index]['VarCharValue'])}: "

        try:
            row_column_data = row_data[column_index]
            if row_column_data is not None:
                row_string += f"{row_column_data['VarCharValue']}"
            else:
                row_string += "null"
        except (TypeError, KeyError) as e:
            row_string += "null"

        if (column_index + 1) < len(row_data):
            row_string += ", "

    return row_string


def generate_sql_verification_data(sql_results_instance):
    """Generates the verification text for a given result sql.

    Keyword arguments:
    sql_results_instance -- the sql results returned from athena
    show_columns -- boolean for whether to show column names or not in the results
    """
    results_string = ""
    all_rows = sql_results_instance["ResultSet"]["Rows"]
    for row_index in range(1, len(all_rows)):
        results_string += f"{generate_sql_verification_data_row(all_rows[row_index])}"
        if (row_index + 1) < len(all_rows):
            results_string += "\n"

    return results_string


def generate_sql_verification_data_row(row):
    """Generates the verification text for a given result sql.

    Keyword arguments:
    row -- the row results from athena
    """
    row_string = ""
    row_data = row["Data"]
    for column_index in range(0, len(row_data)):
        if row_data is not None:
            if "VarCharValue" in row_data[column_index]:
                row_string += f"{row_data[column_index]['VarCharValue']}"
            else:
                row_string += "null"

            if (column_index + 1) < len(row_data):
                row_string += ", "

    return row_string


def generate_json_formatted_results(results_array, executed_by):
    """Generates the results json for the given set of results.

    Keyword arguments:
    results_array -- an array of results, where each item is an array of [manifest_details_array, results_json]
    executed_by -- the unique name of what executed the queries
    """
    query_results_json_object = {}
    query_results_json_array = []

    for results_instance in results_array:
        query_results_json_array.append(
            generate_json_formatted_result(results_instance)
        )

    query_results_json_object["executed_by"] = executed_by
    query_results_json_object["execution_time"] = str(
        datetime.datetime.now().timestamp()
    )
    query_results_json_object["query_results"] = query_results_json_array

    return query_results_json_object


def generate_json_formatted_result(results_instance):
    """Generates the results json for a given result.

    Keyword arguments:
    results_instance -- results as an array of [manifest_details_array, results_json]
    """
    query_result_json_object = {}

    query_result_json_object["query_details"] = results_instance[0]
    query_result_json_object["query_results"] = generate_json_formatted_sql_results(
        results_instance[1]
    )

    return query_result_json_object


def generate_json_formatted_sql_results(sql_results_instance):
    """Generates the results json for a given result sql.

    Keyword arguments:
    sql_results_instance -- the sql results returned from athena
    """
    row_json_array = []
    all_rows = sql_results_instance["ResultSet"]["Rows"]

    for row_index in range(1, len(all_rows)):
        row_json_array.append(
            generate_json_formatted_sql_row_object(all_rows[row_index], all_rows[0])
        )

    return row_json_array


def generate_json_formatted_sql_row_object(row, column_row):
    """Generates the results json for a given result sql.

    Keyword arguments:
    row -- the row results from athena
    column_row -- the first row from athena result set (contains columns, not data) - if nothing, no columns shown
    """
    row_json_object = {}
    row_data = row["Data"]
    column_data = column_row["Data"]

    for column_index in range(0, len(row_data)):
        header_column_data = column_data[column_index]
        if header_column_data is not None and "VarCharValue" in header_column_data:
            row_column_data = row_data[column_index]
            if row_column_data is not None and "VarCharValue" in row_column_data:
                row_json_object[header_column_data["VarCharValue"]] = row_column_data[
                    "VarCharValue"
                ]
            else:
                row_json_object[header_column_data["VarCharValue"]] = "null"

    return row_json_object


def get_desired_asg_count(topic_list, max_size):
    """Create the asg count to use as a string.

    Arguments:
        topic_list (array): The topics to use
        max_size (string): The max size of the asg

    """
    min_number = min(int(max_size), len(topic_list))
    return str(min_number)


def generate_s3_prefix_for_manifest_input_files(base_prefix, manifests_source):
    """Returns the qualified s3 path to the manifest input files.

    Arguments:
        base_prefix (string): The base prefix of all the manifest files
        manifests_source (string): "streaming_main", "streaming_equality", "historic", "full or "incremental"

    """
    if manifests_source.lower() == "streaming_main":
        qualified_source = "streaming/main"
    elif manifests_source.lower() == "streaming_equality":
        qualified_source = "streaming/equality"
    else:
        qualified_source = manifests_source.lower()

    return os.path.join(base_prefix, qualified_source)


def generate_s3_prefix_for_manifest_output_files(
    base_prefix, import_source, export_source
):
    """Returns the qualified s3 path to the manifest output files.

    Arguments:
        base_prefix (string): The base prefix of all the manifest files
        import_source (string): "streaming_main", "streaming_equality", "historic" for import manifest
        export_source (string): "full" or "incremental" for export manifests

    """
    return os.path.join(
        base_prefix,
        f"{import_source.lower()}_{export_source.lower()}",
    )


def generate_manifest_table_name(
    database_name, base_table_name, import_source, export_source
):
    """Returns the qualified manifest table name.

    Arguments:
        database_name (string): The name of the database
        base_table_name (string): The base name of the table
        import_source (string): "streaming_main", "streaming_equality", "historic" for import manifest (can be None)
        export_source (string): "full" or "incremental" for export manifests (can be None)

    """
    table_suffix = ""
    if import_source is not None:
        table_suffix += f"_{import_source.lower()}"
    if export_source is not None:
        table_suffix += f"_{export_source.lower()}"

    return f"{database_name}.{base_table_name}{table_suffix}"


def validate_manifest_sources(import_source, export_source):
    """Validates the manifest sources as valid inputs for manifest comparison.

    Arguments:
        import_source (string): "streaming_main", "streaming_equality", "historic" for import manifest
        export_source (string): "full" or "incremental" for export manifests

    """
    allowed_manifests_sources_import = [
        "streaming_main",
        "streaming_equality",
        "historic",
    ]
    allowed_manifests_sources_export = ["full", "incremental"]

    if import_source.lower() not in allowed_manifests_sources_import:
        raise AssertionError(
            f"{import_type} is not a valid manifest comparison import source"
        )
    elif export_source.lower() not in allowed_manifests_sources_export:
        raise AssertionError(
            f"{export_source} is not a valid manifest comparison export source"
        )
