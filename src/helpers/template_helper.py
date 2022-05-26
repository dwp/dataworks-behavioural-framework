import os
import re
import uuid
from string import Template
from helpers import aws_helper, console_printer


def generate_topic_names(test_run_name, number_of_topics, db_name, unique_names=False):
    """Returns an array of topic names unique to this test run.

    Keyword arguments:
    test_run_name -- the unique id for this test run
    number_of_topics -- number of topics to generate
    db_name -- name of the database
    unique_names -- if True adds an id to the end of the topic names
    """
    topics = []
    for topic_number in range(1, number_of_topics + 1):
        short_topic_name = "{0}.{1}_{2}".format(
            db_name, get_short_topic_name(test_run_name), str(topic_number)
        )

        if unique_names:
            uuid_str = str(uuid.uuid4()).replace("-", "_")
            short_topic_name = "{0}_{1}".format(short_topic_name, uuid_str)

        topics.append(get_topic_name(short_topic_name))

    return topics


def get_dlq_topic_name():
    """Returns the collection name for the DLQ."""
    return "dataworks.ucfs-business-data-event-dlq"


def get_topic_name(test_run_name):
    """Returns the collection name for the DLQ.

    Keyword arguments:
    test_run_name -- the unique id for this test run
    """
    if test_run_name.startswith("db."):
        return test_run_name
    elif test_run_name.startswith("data."):
        return test_run_name

    return f"db.{test_run_name}"


def get_short_topic_name(test_run_name):
    """Returns the collection name for the DLQ.

    Keyword arguments:
    test_run_name -- the unique id for this test run
    """
    return test_run_name[3:] if test_run_name.startswith("db.") else test_run_name


def remove_any_pipe_values_from_topic_name(topic_name):
    """Removes any passed in piped values if they exist from the topic name.

    Keyword arguments:
    topic_name -- the topic name
    """
    return topic_name.split("|")[0] if "|" in topic_name else topic_name


def get_database_and_collection_from_topic_name(topic_name):
    """Returns the collection name parsed from the topic (with or without 'db.' in front of it).

    Keyword arguments:
    topic_name -- the topic name to get the collection from
    """
    short_topic_name = get_short_topic_name(topic_name)
    return (
        (None, short_topic_name)
        if "." not in short_topic_name
        else (short_topic_name.split(".")[0], short_topic_name.split(".")[1])
    )


def generate_synthetic_data_topic_names(s3_prefix, datasets_bucket):
    """Returns an array of topic names unique to  synthetic-data-ingestion.

    Keyword arguments:
    prefix -- S3 prefix of the synthetic raw data
    datasets_bucket --  S3 bucket that holds the synthetic raw data
    """
    pattern = f"{s3_prefix}/(?P<database>[\w-]+)\.(?P<collection>[\w-]+)\.(?P<number>[0-9]+)\.(json)\.(gz)"
    s3_client = aws_helper.get_client(service_name="s3")
    topics = set()
    for file in aws_helper.retrieve_files_from_s3_with_bucket_and_path(
        s3_client, datasets_bucket, s3_prefix
    ):
        key = file["Key"]
        matched_key = re.match(pattern, key)
        topic = f"db.{matched_key.group(1)}.{matched_key.group(2)}"
        topics.add(topic)

    return topics


def get_historic_data_importer_prefixes(
    prefixes_list, use_one_message_per_override_path
):
    """Returns an array of the prefixes for HDI using test run name or the overrides.

    Keyword arguments:
    prefixes_override -- The prefix or prefixes as comma delimited list
    use_one_message_per_override_path -- True if using an override to split the prefixes by comma
    """
    if use_one_message_per_override_path:
        console_printer.print_info(
            f"Sending one message per path using '{prefixes_list}' to HDI"
        )
        return prefixes_list.split(",")
    else:
        console_printer.print_info(
            f"Sending one message overall using '{prefixes_list}' for HDI"
        )
        return [prefixes_list]


def get_hbase_table_name_fromt_topic_name(topic_name):
    """Returns a qualified HBASE table name from a topic name.

    Keyword arguments:
    topic_name -- the topic name
    """
    pattern = f"^(?:\w*\.)?([-\w]+)\.([-\w]+)$"
    matched = re.match(pattern, topic_name)
    table_name = f"{matched.group(1)}:{matched.group(2)}"
    return table_name