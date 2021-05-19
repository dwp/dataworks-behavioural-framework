import time
from helpers import aws_helper, date_helper, console_printer, template_helper


def delete_item_in_export_status_table(
    export_status_table_name, topic_name, correlation_id
):
    """Deletes an export status from dynamodb.

    Keyword arguments:
    export_status_table_name -- the export table name
    topic_name -- the topic name
    correlation_id -- the correlation id
    """
    key_dict = {
        "CorrelationId": {"S": f"{correlation_id}"},
        "CollectionName": {"S": f"{topic_name}"},
    }

    aws_helper.delete_item_from_dynamodb(export_status_table_name, key_dict)


def update_item_in_export_status_table(
    export_status_table_name,
    topic_name,
    correlation_id,
    status,
    files_exported_count,
    files_sent_count,
    files_received_count,
    export_date,
):
    """Updates an export status in dynamodb.

    Keyword arguments:
    export_status_table_name -- the export table name
    topic_name -- the topic name
    correlation_id -- the correlation id
    export_date -- the export date
    """
    time_to_live = str(date_helper.get_current_epoch_seconds)

    key_dict = {
        "CorrelationId": {"S": f"{correlation_id}"},
        "CollectionName": {"S": f"{topic_name}"},
    }

    update_expression = "SET CollectionStatus = :s, FilesExported = :e, FilesSent = :f, FilesReceived = :g, ExportDate = :h"

    expression_attribute_values_dict = {
        ":s": {"S": f"{status}"},
        ":e": {"N": f"{files_exported_count}"},
        ":f": {"N": f"{files_sent_count}"},
        ":g": {"N": f"{files_received_count}"},
        ":h": {"S": f"{export_date}"},
    }

    aws_helper.update_item_in_dynamo_db(
        export_status_table_name,
        key_dict,
        update_expression,
        expression_attribute_values_dict,
    )


def wait_for_statuses_in_export_status_table(
    timeout, export_status_table_name, topics, correlation_id, desired_statuses
):
    """Returns true or false for if the items for the given correlation id after waiting for the timeout and topic list match given status.

    Keyword arguments:
    timeout -- the timeout in seconds
    export_status_table_name -- the export table name
    topics -- the array of topics to check
    correlation_id -- the correlation id
    desired_statuses -- an array of allowed statuses
    """
    count = 1
    matched_topics = []

    console_printer.print_info(
        f"Checking all export statuses for all topics match one of the desired statuses of '{desired_statuses}' for correlation_id of '{correlation_id}'"
    )

    while len(matched_topics) != len(topics) and count <= timeout:
        for topic in topics:
            if topic not in matched_topics:
                topic_name = template_helper.get_topic_name(topic)

                key_dict = {
                    "CorrelationId": {"S": f"{correlation_id}"},
                    "CollectionName": {"S": f"{topic_name}"},
                }

                item_details = aws_helper.get_item_from_dynamodb(
                    export_status_table_name, key_dict
                )
                if "Item" not in item_details:
                    console_printer.print_debug(
                        f"No export status found for key dict of '{key_dict}'"
                    )
                    continue

                collection_status = item_details["Item"]["CollectionStatus"]["S"]
                if collection_status not in desired_statuses:
                    console_printer.print_debug(
                        f"Status was '{collection_status}' which did not match any of '{desired_statuses}'"
                    )
                    continue

                console_printer.print_info(
                    f"All export statuses match one of the desired statuses of '{desired_statuses}' for correlation_id of '{correlation_id}' and topic '{topic_name}'"
                )
                matched_topics.append(topic)
        time.sleep(1)
        count += 1

    if len(matched_topics) != len(topics):
        console_printer.print_info(
            f"All export statuses for one or more topics did match one of the desired statuses of '{desired_statuses}' for correlation_id of '{correlation_id}' after '{timeout}' seconds"
        )
        return False

    console_printer.print_info(
        f"All export statuses match one of the desired statuses of '{desired_statuses}' for correlation_id of '{correlation_id}'"
    )
    return True
