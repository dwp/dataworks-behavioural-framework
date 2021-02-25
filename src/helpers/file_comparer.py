import time
import json
import os
from concurrent.futures import ThreadPoolExecutor, wait
from helpers import file_helper, aws_helper, console_printer, template_helper


def assert_specific_file_stored_in_hbase_threaded(
    topics, output_folder, timeout, record_expected_in_hbase=True, wrap_id=False
):
    """Checks the specific files in stored in HBase for the given topics and raises assertion errors if not using threads.

    Keyword arguments:
    topics -- full topic names as an array
    output_folder -- the output folder base for the generated historic data
    timeout -- the timeout in seconds
    record_expected_in_hbase -- true if the record should be in HBase and false if it should not (default True)
    wrap_id -- True is the id format should be wrapped with an "id" object (default False)
    """
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_results = []

        for topic in topics:
            console_printer.print_info(f"Looking for HBase data for topic {topic}")
            output_folder_qualified = os.path.join(output_folder, topic)
            topic_qualified = template_helper.get_topic_name(topic)
            for output_file in os.listdir(output_folder_qualified):
                future_results.append(
                    executor.submit(
                        assert_specific_file_stored_in_hbase,
                        topic_qualified,
                        os.path.join(output_folder_qualified, output_file),
                        timeout,
                        record_expected_in_hbase=record_expected_in_hbase,
                        wrap_id=wrap_id,
                    )
                )

        wait(future_results)
        for future in future_results:
            try:
                yield future.result()
            except Exception as ex:
                raise AssertionError(ex)


def assert_specific_file_stored_in_hbase(
    topic_name, file_full_path, timeout, record_expected_in_hbase=True, wrap_id=False
):
    """Checks the specific file in stored in HBase and raises assertion error if not.

    Keyword arguments:
    topic_name -- full topic name
    file_full_path -- the full path and name of the file to check
    timeout -- the timeout in seconds
    record_expected_in_hbase -- true if the record should be in HBase and false if it should not (default True)
    wrap_id -- True is the id format should be wrapped with an "id" object (default False)
    """
    file_contents = file_helper.get_contents_of_file(file_full_path, True)
    id_object = file_helper.get_id_object_from_json_file(file_full_path)

    input_formatted = file_helper.get_json_with_replaced_values(file_contents)

    console_printer.print_info(
        f"Retrieving specific file from HBase using topic name '{topic_name}', "
        + f"id of '{id_object}', expected setting of '{record_expected_in_hbase}' "
        + f"and wrap id setting of '{wrap_id}'"
    )

    file_found = False
    file_matches = False
    count = 1
    while (
        not file_found or (record_expected_in_hbase and not file_matches)
    ) and count <= timeout:
        hbase_data = aws_helper.retrieve_data_from_hbase(id_object, topic_name, wrap_id)
        if hbase_data:
            file_found = True
            hbase_data_json = json.loads(hbase_data)
            hbase_contents = json.dumps(hbase_data_json, sort_keys=True)
            hbase_formatted = file_helper.get_json_with_replaced_values(hbase_contents)
            if input_formatted == hbase_formatted:
                file_matches = True
        time.sleep(1)
        count += 1

    if record_expected_in_hbase:
        if not file_found:
            raise AssertionError(
                f"File {file_full_path}: content '{input_formatted}' not found in HBase"
            )
        elif not file_matches:
            raise AssertionError(
                f"Input mismatch for file '{file_full_path}': content '{input_formatted}' not matched by HBase content '{hbase_formatted}'"
            )
    elif file_found:
        raise AssertionError(
            f"File '{file_full_path}': content '{input_formatted}' found in HBase"
        )
    elif file_matches:
        raise AssertionError(
            f"Input mismatch for file '{file_full_path}': content '{input_formatted}' matched by HBase content '{hbase_formatted}'"
        )

    return topic_name


def assert_specific_id_missing_in_hbase(topic_name, id_object, timeout, wrap_id):
    """Checks the specific file is not stored in HBase and raises assertion error if it is.

    Keyword arguments:
    topic_name -- full topic name
    id_object -- the id to look for
    timeout -- the timeout in seconds
    wrap_id -- if id is a string, will need to be wrapped for the retriever check
    """
    console_printer.print_info(
        f"Checking specific id missing from HBase using topic name '{topic_name}', "
        + f"id of '{id_object}' and wrap id setting of '{wrap_id}'"
    )

    count = 1
    while count <= timeout:
        if aws_helper.retrieve_data_from_hbase(id_object, topic_name, wrap_id):
            raise AssertionError(f"Id matching '{id_object}' present in HBase")

        time.sleep(1)
        count += 1
