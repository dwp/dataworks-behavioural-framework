import json
import pathlib

from helpers import aws_helper, console_printer


def read_test_data(file_name):
    home_dir = pathlib.Path(__file__).resolve().parents[2]
    test_data_location = (
        "src/fixture-data/functional-tests/dataworks_kafka_consumer_data"
    )
    file_path = f"{home_dir}/{test_data_location}/{file_name}"
    console_printer.print_info(f"Read test data from file path: {file_path}")
    fh = open(file_path, "r")
    input_data = fh.read()
    messages = json.loads(input_data)
    return messages


def get_parquet_file_count(bucket_name, prefix):
    count = aws_helper.get_number_of_s3_objects_for_key(bucket_name, prefix)
    return count
