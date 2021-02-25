import json
from helpers import (
    invoke_lambda,
    console_printer,
    template_helper,
    file_helper,
)


def get_metadata_for_specific_id_in_topic(table_name, id_string, topic_name):
    """Returns the metadata for a given id.

    Arguments:
    table_name -- the table name to check
    id_string -- the json dumped id string
    topic_name -- the topic name to get metadata for
    """
    console_printer.print_info(
        f"Retrieving metadata for id of '{id_string}' in metadata table '{table_name}' with topic name of '{topic_name}'"
    )

    qualified_topic_name = template_helper.get_topic_name(topic_name)

    payload_dict = {
        "table-name": table_name,
        "hbase-id-like": id_string,
        "topic-name-equals": qualified_topic_name,
    }
    payload_json = json.dumps(payload_dict)

    return invoke_lambda.invoke_ingestion_metadata_query_lambda(payload_json)


def get_metadata_for_specific_id_and_timestamp_in_topic(
    table_name, id_string, timestamp, topic_name
):
    """Returns the metadata for a given id and timestamp.

    Arguments:
    table_name -- the table name to check
    id_string -- the json dumped id string
    timestamp -- the timestamp as an int
    topic_name -- the topic name to get metadata for
    """
    console_printer.print_info(
        f"Retrieving metadata for id of '{id_string}' in metadata table '{table_name}' with topic name of '{topic_name}' and timestamp of '{str(timestamp)}'"
    )

    qualified_topic_name = template_helper.get_topic_name(topic_name)

    payload_dict = {
        "table-name": table_name,
        "hbase-id-like": id_string,
        "topic-name-equals": qualified_topic_name,
        "hbase-timestamp-equals": timestamp,
    }
    payload_json = json.dumps(payload_dict)

    return invoke_lambda.invoke_ingestion_metadata_query_lambda(payload_json)


def get_metadata_for_id_and_timestamp_from_file(
    table_name, file_path, topic_name, wrap_id=False
):
    """Returns the metadata for a given id and a tuple of the id and timestamp searched for.

    Arguments:
    table_name -- the table name to check
    file_path -- the file containing the id
    topic_name -- the topic name to get metadata for
    wrap_id -- True is the id format should be wrapped with an "id" object (default False)
    """
    console_printer.print_info(
        f"Retrieving metadata for id from file in '{file_path}' in metadata table '{table_name}' with topic name of '{topic_name}'"
    )

    qualified_topic_name = template_helper.get_topic_name(topic_name)
    record_id = file_helper.get_id_object_from_json_file(file_path)
    record_timestamp = file_helper.get_timestamp_as_long_from_json_file(file_path)
    id_string = json.dumps(record_id)

    if wrap_id:
        id_string = json.dumps({"id": id_string})

    id_string_qualified = id_string.replace(" ", "")

    results = get_metadata_for_specific_id_and_timestamp_in_topic(
        table_name, id_string_qualified, record_timestamp, qualified_topic_name
    )

    return (id_string_qualified, record_timestamp, results)
