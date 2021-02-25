import os
import json
from helpers import console_printer, template_helper


def generate_correct_manifest_line(
    record_id, record_timestamp, topic_name, wrap_id=False
):
    """Returns the data that would be in a successful manifest for the given id.

    Keyword arguments:
    record_id -- the id of the record
    record_timestamp -- the timestamp of the record
    topic_name -- the topic name
    wrap_id -- True is the id format should be wrapped with an "id" object (default False)
    """
    id_string = json.dumps(record_id)

    if wrap_id:
        id_string = json.dumps({"id": id_string})

    id_string_qualified = id_string.replace(" ", "")

    (
        database,
        collection,
    ) = template_helper.get_database_and_collection_from_topic_name(topic_name)

    return f'"{id_string_qualified}"|{record_timestamp}|{database}|{collection}|STREAMED|K2HB|"{id_string_qualified}"|KAFKA_RECORD'
