from datetime import timedelta
import datetime
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
    filenames.append(str(datetime.date.today() - timedelta(31)))
    filenames_zip_s3 = [filenames_prefix + "-" + k + ".zip" for k in filenames]
    filenames_zip_s3.sort(reverse=False)
    filenames_csv_local = [
        os.path.join(output_folder, filenames_prefix + "-" + k + ".csv")
        for k in filenames
    ]
    filenames_zip_local = [
        os.path.join(output_folder, filenames_prefix + "-" + k + ".zip")
        for k in filenames
    ]
    filenames_csv_local.sort(reverse=False)
    filenames_zip_local.sort(reverse=False)

    return filenames_zip_s3, filenames_csv_local, filenames_zip_local



def gen_string():
    letters = string.ascii_letters
    result_str = "".join(rd.choice(letters) for i in range(8))
    return rd.choice([result_str, None])


def gen_int():
    return str(rd.randint(0, 100))


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
        header_record = [k for k, v in cols.items()]
        writer.writerow(header_record)
        gb = convert_to_gigabytes(os.stat(filename).st_size)
        while gb <= desired_gb:
            record_data = [
                gen_string() if v == "string" else gen_int() for k, v in cols.items()
            ]
            writer.writerow(record_data)
            gb = convert_to_gigabytes(os.stat(filename).st_size)


def generate_csv_file_row_with_missing_field(filename, desired_gb, cols):
    with open(filename, "w+", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=",")
        header_record = [k for k, v in cols.items()]
        writer.writerow(header_record)
        r = [gen_string() if v == "string" else gen_int() for k, v in cols.items()]
        writer.writerow(r[:-1])
        gb = convert_to_gigabytes(os.stat(filename).st_size)
        while gb <= desired_gb:
            record_data = [
                gen_string() if v == "string" else gen_int() for k, v in cols.items()
            ]
            writer.writerow(record_data)
            gb = convert_to_gigabytes(os.stat(filename).st_size)


def generate_csv_file_string_instead_of_int(filename, desired_gb, cols):
    with open(filename, "w+", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=",")
        header_record = [k for k, v in cols.items()]
        writer.writerow(header_record)
        gb = convert_to_gigabytes(os.stat(filename).st_size)
        while gb <= desired_gb:
            record_data = [gen_string() for k, v in cols.items()]
            writer.writerow(record_data)
            gb = convert_to_gigabytes(os.stat(filename).st_size)


def download_file(bucket, prefix, filename, local_folder):
    s3 = aws_helper.get_client("s3")
    s3.download_file(
        bucket, os.path.join(prefix, filename), os.path.join(local_folder, filename)
    )


def s3_upload(context, local_filename, prefix, bucket_filename):
    console_printer.print_info(
        f"Uploading the local file {local_filename} as {bucket_filename} into s3 bucket {context.data_ingress_stage_bucket} "
    )
    aws_helper.upload_file_to_s3_and_wait_for_consistency(local_filename, context.data_ingress_stage_bucket, 60, os.path.join(prefix, bucket_filename))


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


def did_alarm_trigger(alarm_name):

    client = aws_helper.get_client("cloudwatch")
    response = client.describe_alarm_history(
        AlarmName=alarm_name,
        HistoryItemType="StateUpdate",
        StartDate=datetime.datetime.today() - timedelta(days=1),
        MaxRecords=99,
        ScanBy="TimestampDescending",
    )
    utc = pytz.UTC
    w = "Alarm updated from INSUFFICIENT_DATA to ALARM"
    x = utc.localize(datetime.datetime.now() - timedelta(minutes=65))
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
