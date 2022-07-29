import datetime
from datetime import timedelta
from configparser import ConfigParser
from helpers import (
    console_printer,
    file_helper,
    historic_data_load_generator,
    aws_helper,
)
import csv
import os
import json
import string
import boto3
import sys
import random as rd
from boto3.dynamodb.conditions import Key
import subprocess


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


def get_filenames_today_yesterday(filenames_prefix, files_per_date, output_folder):
    r = range(files_per_date)
    t1 = [
        filenames_prefix + "-" + str(datetime.date.today()) + "-" + f"part{i}.csv"
        for i in r
    ]
    t0 = [
        filenames_prefix
        + "-"
        + str(datetime.date.today() - timedelta(1))
        + "-"
        + f"part{j}.csv"
        for j in r
    ]
    res = t1 + t0
    return [os.path.join(output_folder, k) for k in res]


def gen_string():
    letters = string.ascii_letters
    result_str = "".join(rd.choice(letters) for i in range(8))
    return rd.choice([result_str, None])


def get_gen_files_keys(filenames, local_output_folder, nrecords, cols):
    generate_csv_files(filenames, local_output_folder, nrecords, cols)
    return [
        os.path.join(local_output_folder, x) for x in os.listdir(local_output_folder)
    ]


def generate_csv_files(filenames, nrecords, cols):
    for i in filenames:
        with open(i, "w+", newline="") as csvfile:
            writer = csv.writer(csvfile, delimiter=",")
            header_record = cols
            writer.writerow(header_record)
            num = 1
            while num <= int(nrecords):
                record_data = [gen_string() for i in cols]
                writer.writerow(record_data)
                num += 1


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
        f"Uploading the local file {filename} with basename as {file_name} into s3 bucket {context.published_bucket} using key name as {inputs_s3_key}"
    )
    aws_helper.put_object_in_s3(
        input_file, context.data_ingress_stage_bucket, inputs_s3_key
    )


def dynamo_table(context):
    console_printer.print_info("getting dynamodb table")
    dynamodb = aws_helper.get_resource("dynamodb")
    return dynamodb.Table(context.args_ch["audit-table"]["name"])


def filename_latest_dynamo_add(context):
    table = dynamo_table(context)
    myitem = {
        context.args_ch["audit-table"]["hash_key"]: context.args_ch["audit-table"][
            "hash_id"
        ],
        context.args_ch["audit-table"]["range_key"]: context.args_ch["audit-table"][
            "data_product_name"
        ],
        "Latest_File": str(datetime.date.today() - timedelta(1)) + "-" + "part0",
        "CumulativeSizeBytes": "123",
    }
    aws_helper.insert_item_to_dynamo_db_v2(table, myitem)


def file_latest_dynamo_fetch(context):
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
