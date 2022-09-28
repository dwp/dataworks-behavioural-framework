import datetime
from datetime import timedelta
import pytz
from configparser import ConfigParser
from helpers import (
    console_printer,
    file_helper,
    historic_data_load_generator,
    aws_helper,
)
import csv
import os
import string
import sys
import random as rd
from boto3.dynamodb.conditions import Key


def get_args(location: str):
    console_printer.print_info(f"getting args from file {location}")
    try:
        conf = ConfigParser()
        conf.read(location)
        return conf
    except Exception as ex:
        console_printer.print_error_text(
            f"failed to load configuration file {location} due to {ex}"
        )
        sys.exit(-1)


def get_filenames(filenames_prefix, output_folder):
    filenames = [str(datetime.date.today())]

    for i in [31, 0]:
        filenames.append(str(datetime.date.today() - timedelta(i)))
    filenames = [
        os.path.join(output_folder, filenames_prefix + "-" + k + ".csv")
        for k in filenames
    ]
    return filenames


def gen_string():
    letters = string.ascii_letters
    result_str = "".join(rd.choice(letters) for i in range(8))
    return rd.choice([result_str, None])


def convert_to_gigabytes(bytes):
    try:
        constant = 1073741824
        gb = round(bytes / constant, 4)
        return gb
    except Exception as ex:
        console_printer.print_error_text(f"couldn't covert to gb due to {ex}")
        sys.exit(-1)


def generate_csv_file(filename, desired_gb, cols):
    with open(filename, "w+", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=",")
        header_record = cols
        writer.writerow(header_record)
        gb = convert_to_gigabytes(os.stat(filename).st_size)
        while gb <= desired_gb:
            record_data = [gen_string() for i in cols]
            writer.writerow(record_data)
            gb = convert_to_gigabytes(os.stat(filename).st_size)


def download_file(bucket, prefix, filename, local_folder):
    s3 = aws_helper.get_client("s3")
    s3.download_file(
        bucket, os.path.join(prefix, filename), os.path.join(local_folder, filename)
    )


def s3_upload(context, filename, prefix):
    console_printer.print_info(f"The file name is {filename}")
    file_name = os.path.basename(filename)
    input_file = file_helper.get_contents_of_file(filename, False)
    inputs_s3_key = os.path.join(prefix, file_name)
    console_printer.print_info(
        f"Uploading the local file {filename} with basename as {file_name} into s3 bucket {context.published_bucket} "
        f"using key name as {inputs_s3_key}"
    )
    aws_helper.put_object_in_s3(
        input_file, context.data_ingress_stage_bucket, inputs_s3_key
    )


def dynamo_table(context):
    console_printer.print_info("getting dynamodb table")
    dynamodb = aws_helper.get_resource("dynamodb")
    return dynamodb.Table(context.args_ch["audit-table"]["name"])


def add_latest_file(context, filename):
    table = dynamo_table(context)
    myitem = {
        context.args_ch["audit-table"]["hash_key"]: context.args_ch["audit-table"][
            "hash_id"
        ],
        context.args_ch["audit-table"]["range_key"]: context.args_ch["audit-table"][
            "data_product_name"
        ],
        "Latest_File": filename,
        "CumulativeSizeBytes": "123",
    }
    aws_helper.insert_item_to_dynamo_db_v2(table, myitem)


def get_latest_file(context):
    console_printer.print_info("getting last imported file name from dynamo")
    try:
        table = dynamo_table(context)
        response = table.query(
            KeyConditionExpression=Key(context.args_ch["audit-table"]["hash_key"]).eq(
                context.args_ch["audit-table"]["hash_id"]
            ),
            ScanIndexForward=False,
        )
        if not response["Items"]:
            console_printer.print_error_text("couldn't find any items for set hash key")
            sys.exit(-1)
        else:
            return response["Items"][0]["Latest_File"]
    except Exception as ex:
        console_printer.print_error_text(
            f"failed to fetch last filename imported due to {ex}"
        )
        sys.exit(-1)


def did_file_size_alarm_went_on(alarm_name):

    client = aws_helper.get_client("cloudwatch")
    response = client.describe_alarm_history(
        AlarmName=alarm_name,
        HistoryItemType="StateUpdate",
        StartDate=datetime.today() - timedelta(days=1),
        MaxRecords=99,
        ScanBy="TimestampDescending",
    )
    utc = pytz.UTC
    w = "Alarm updated from INSUFFICIENT_DATA to ALARM"
    x = utc.localize(datetime.now() - timedelta(minutes=62))
    if (
        len(
            [
                i
                for i in response["AlarmHistoryItems"]
                if i["HistorySummary"] == w and i["Timestamp"] > x
            ]
        )
        >= 1
    ):
        return True
    else:
        return False
