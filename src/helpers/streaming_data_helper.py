import os
import uuid


def generate_fixture_data_folder(message_type):
    """Generates the fixture data folder for the given message type

    Keyword arguments:
    message_type -- the message type in use (kafka_main, kafka_equalities, kafka_audit, corporate_data)
    """
    if message_type.lower() == "kafka_main":
        return os.path.join("kafka_data", "main")
    elif message_type.lower() == "kafka_equalities":
        return os.path.join("kafka_data", "equalities")
    elif message_type.lower() == "kafka_audit":
        return os.path.join("kafka_data", "audit")
    elif message_type.lower() == "claimant_api":
        return os.path.join("kafka_data", "claimant_api")

    return "corporate_data"


def get_metadata_store_table_name(friendly_table_name, table_names_array):
    """Returns the actual metadata store name for the friendly name

    Keyword arguments:
    friendly_table_name -- the friendly table name used in the feature files, i.e. main, audit or equalities
    table_names_array -- the table names array from the context
    """
    for table_name_details in table_names_array:
        if table_name_details[0].lower() == friendly_table_name.lower():
            return table_name_details[1].lower()

    friendly_table_names_allowed = [
        table_name_details[0].lower() for table_name_details in table_names_array
    ]
    raise AssertionError(
        f"'{friendly_table_name}' is not a valid friendly name for any metadata store table, the allowed values are '{friendly_table_names_allowed}'"
    )


def generate_topic_prefix(message_type):
    """Generates the topic prefix for the given message type

    Keyword arguments:
    message_type -- the message type in use (kafka_main, kafka_equalities, kafka_audit, corporate_data)
    """
    if (
        message_type.lower() == "kafka_equalities"
        or message_type.lower() == "kafka_audit"
    ):
        return ""

    return "db."


def generate_topics_override(message_type, current_topics):
    """Generates the topics for the given message type as an array

    Keyword arguments:
    message_type -- the message type in use (kafka_equalities, kafka_audit, first_topic)
    current_topics -- if not a valid topic type, then this will be used instead
    """
    if message_type.lower() == "kafka_equalities":
        return [{"topic": "data.equality", "key": str(uuid.uuid4())}]
    elif message_type.lower() == "kafka_audit":
        return [{"topic": "data.businessAudit", "key": str(uuid.uuid4())}]
    elif message_type.lower() == "first_topic":
        return [current_topics[0]]

    return current_topics
