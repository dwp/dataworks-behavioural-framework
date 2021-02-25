import os
import datetime
from helpers import console_printer, template_helper


def generate_arguments_for_historic_data_load(
    correlation_id,
    topics,
    s3_base_prefix,
    s3_suffix,
    default_topic_list,
    skip_earlier_than,
    skip_later_than,
):
    """Works out the topics needed for snapshot sender based on the passed in overrides.

    Keyword arguments:
    correlation_id -- unique id for this test run
    topics -- comma delimited list of topics or "ALL" the use default list
    s3_base_prefix -- the s3 location in the bucket to load files from
    s3_suffix -- comma delimited list of suffixes to add to the prefix or None
    default_topic_list -- if topics is ALL then this comma delimited list is used for the topics
    skip_earlier_than -- format of date time must be `yyyy-MM-dd'T'HH:mm:ss.SSS` with an optional literal `Z` at the end (or None)
    skip_later_than -- format of date time must be `yyyy-MM-dd'T'HH:mm:ss.SSS` with an optional literal `Z` at the end (or None)
    """
    if not s3_suffix:
        s3_full_prefix = s3_base_prefix
    elif "," in s3_suffix:
        all_prefixes = []
        for s3_single_suffix in s3_suffix.split(","):
            all_prefixes.append(os.path.join(s3_base_prefix, s3_single_suffix))
        s3_full_prefix = ",".join(all_prefixes)
    else:
        s3_full_prefix = os.path.join(s3_base_prefix, s3_suffix)

    console_printer.print_info(
        f"Attempting to generate arguments for historic data load"
    )
    console_printer.print_info(
        f"Topics list is '{topics}', s3 base prefix is '{s3_full_prefix}', correlation id is '{correlation_id}', skip earlier than is '{skip_earlier_than}' and skip later than is '{skip_later_than}'"
    )

    topics_qualified = default_topic_list if topics.lower() == "all" else topics

    return f"{topics_qualified} {s3_full_prefix} {skip_earlier_than} {skip_later_than} {correlation_id}"


def generate_arguments_for_corporate_data_load(
    correlation_id,
    topics,
    s3_base_prefix,
    metadata_table_name,
    default_topic_list,
    file_pattern,
    skip_earlier_than,
    skip_later_than,
    partition_count,
    prefix_per_execution,
):
    """Works out the topics needed for snapshot sender based on the passed in overrides.

    Keyword arguments:
    correlation_id -- unique id for this test run
    topics -- comma delimited list of topics or "ALL" the use default list
    s3_base_prefix -- the s3 location in the bucket to load files from
    metadata_table_name -- the table for the metadata store writes
    default_topic_list -- if topics is ALL then this comma delimited list is used for the topics
    file_pattern -- the file pattern for the input files
    skip_earlier_than -- format of date time must be `yyyy-MM-dd` (or None)
    skip_later_than -- format of date time must be `yyyy-MM-dd` (or None)
    partition_count -- number of partitions to split the run by (or None)
    prefix_per_execution -- true (as a string) to enable a prefix per execution when running the jar

    """
    console_printer.print_info(
        f"Attempting to generate arguments for corporate data load"
    )

    console_printer.print_info(
        f"Topics list is '{topics}', s3 base prefix is '{s3_base_prefix}', correlation id is '{correlation_id}',"
        f" file pattern is '{file_pattern}', metadata table name is '{metadata_table_name}',"
        f" prefix per execution setting is '{prefix_per_execution}',"
        f" start date is '{skip_earlier_than}' and end date is '{skip_later_than}'"
    )

    topics_qualified = default_topic_list if topics.lower() == "all" else topics

    start_date = "NOT_SET" if skip_earlier_than is None else skip_earlier_than
    end_date = "NOT_SET" if skip_later_than is None else skip_later_than
    partitions = "NOT_SET" if partition_count is None else partition_count
    per_execution = "NOT_SET" if prefix_per_execution is None else prefix_per_execution

    return f'{topics_qualified} {s3_base_prefix} {metadata_table_name} {correlation_id} "{file_pattern}" {start_date} {end_date} {partitions} {per_execution}'


def generate_corporate_data_s3_prefix(base_prefix, database, collection, timestamp):
    """Generates the S3 prefix to upload a file to for the corporate data.

    Keyword arguments:
    base_prefix -- the base location for the corporate data
    database -- the database for this file
    collection -- the collection name for this file
    timestamp -- the timestamp for the file being sent to s3
    """
    day_padded = "{:02d}".format(timestamp.day)
    month_padded = "{:02d}".format(timestamp.month)

    return os.path.join(
        base_prefix,
        str(timestamp.year),
        str(month_padded),
        str(day_padded),
        database,
        collection,
    )
