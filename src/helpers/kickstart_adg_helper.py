import json
import os
import string
import uuid
import sys
import re
import random as rd
from datetime import datetime, timedelta
from helpers import console_printer, date_helper


def dataGenBigint():
    return rd.getrandbits(32)


def dataGenBoolean():
    return rd.choice([True, False])


def dataGenSmallInt():
    return rd.getrandbits(4)


def dataGenNumeric():
    return float(rd.getrandbits(8))


def dataGenInteger():
    return rd.getrandbits(8)


def dataGenText():
    str_1 = "".join(rd.choice(string.ascii_letters + string.digits) for i in range(20))
    str_2 = "".join(rd.choice(" "))
    str_3 = "".join(rd.choice(string.digits + string.ascii_letters) for i in range(20))
    result_str = " ".join([str_1, str_2, str_3])
    return result_str


def dataGenTimestamp():
    current_date = datetime.today()
    return datetime.strftime(current_date, "%Y-%m-%d %H:%M:%S")


def dataGenUUID():
    return str(uuid.uuid1())


def dataGenString():
    letters = string.ascii_letters
    result_str = "".join(rd.choice(letters) for i in range(8))
    return result_str


def dataGenDate():
    current_date = datetime.today()
    return datetime.strftime(current_date, "%Y-%m-%d 00:00:00")


def dataTypeMapping(type):
    try:
        datatypes = {
            "bigint": dataGenBigint,
            "string": dataGenString,
            "boolean": dataGenBoolean,
            "text": dataGenText,
            "timestamp": dataGenTimestamp,
            "smallint": dataGenSmallInt,
            "uuid": dataGenUUID,
            "date": dataGenDate,
            "numeric": dataGenNumeric,
            "integer": dataGenInteger,
        }

        return datatypes[type]

    except BaseException as ex:
        console_printer.error_text_format(
            f"Error while mapping data types because of error {str(ex)}"
        )
        sys.exit(-1)


def get_schema_config(template_root_path, template_name):
    try:
        template_file = os.path.join(template_root_path, "kickstart_adg", template_name)
        with open(template_file, "r") as json_file:
            schema_config = json.load(json_file)
        return schema_config

    except BaseException as ex:
        console_printer.error_text_format(
            f"Problem while generating kickstart schema because of error {str(ex)}"
        )


def generate_data(module_name, record_count, schema_config, temp_folder):

    console_printer.print_info(
        f"starting the process of data generation for {module_name}. Total {record_count} will be generated each file"
    )

    console_printer.print_info(
        f"extracting the schema level configuration for {module_name}"
    )

    try:
        console_printer.print_info(
            f"extracting all required properties from schema config"
        )

        local_output_folder = os.path.join(temp_folder, module_name)

        console_printer.print_info(
            f"creating the required directory {local_output_folder}"
        )

        os.mkdir(local_output_folder)

        console_printer.print_info(
            "starting the process of generating the random records based on the datatype in template file"
        )

        if schema_config["record_layout"].lower() == "csv":
            for collection, collection_schema in schema_config["schema"].items():
                run_date = datetime.strftime(datetime.now(), "%Y-%m-%d")
                epoc_time = str(date_helper.get_current_epoch_seconds())
                output_file_name = (
                    schema_config["output_file_pattern"]
                    .replace("run-date", run_date)
                    .replace("collection", collection)
                    .replace("epoc-time", epoc_time)
                )

                output_file = os.path.join(local_output_folder, output_file_name)

                console_printer.print_info(
                    f"opening the file {output_file} to write test data"
                )
                with open(output_file, "w+") as writer:
                    header_record = f"{schema_config['record_delimiter']}".join(
                        collection_schema.keys()
                    )
                    writer.write(header_record + "\n")
                    num = 1
                    while num <= int(record_count):
                        record_data = f"{schema_config['record_delimiter']}".join(
                            [
                                str(dataTypeMapping(type)())
                                for key, type in collection_schema.items()
                            ]
                        )
                        writer.write(record_data + "\n")
                        num += 1

        elif schema_config["record_layout"].lower() == "json":

            for collection, collection_schema in schema_config["schema"].items():
                run_date = datetime.strftime(datetime.now(), "%Y-%m-%d")
                epoc_time = str(date_helper.get_current_epoch_seconds())
                output_file_name = (
                    schema_config["output_file_pattern"]
                        .replace("run-date", run_date)
                        .replace("collection", collection)
                        .replace("epoc-time", epoc_time)
                )
                output_file = os.path.join(local_output_folder, output_file_name)
                with open(output_file, "w+") as writer:
                    num = 1
                    data = []
                    while num <= int(record_count):
                        record={}
                        for column, column_property in collection_schema.items():
                            if 'value' in column_property:
                                record.update({ column : {"value" : dataTypeMapping(column_property["value"])()}})
                            if 'pii_flg' in column_property:
                                record[column].update({"pii_flg" : column_property["pii_flg"]})
                        data.append(record)
                        num += 1
                    writer.write(json.dumps(data, indent=4))

        return [
            os.path.join(local_output_folder, x)
            for x in os.listdir(local_output_folder)
        ]

    except BaseException as ex:
        console_printer.error_text_format(
            f"Test run failed while generating the data because of error - {str(ex)}"
        )
        sys.exit(-1)


def generate_hive_queries(schema_config, published_bucket, s3_path):
    try:
        hive_export_list = []
        for collection, collections_schema in schema_config["schema"].items():
            date_uploaded = datetime.strftime(datetime.now(), "%Y-%m-%d")
            file_name = f"e2e_{collection}.csv"
            column_name = ",".join(
                [
                    re.sub("[^0-9a-zA-Z$]+", " ", col).strip().replace(" ", "_").lower()
                    for col in collections_schema.keys()
                ]
            )
            hive_export_bash_command = (
                f"hive -e 'SELECT {column_name} FROM uc_kickstart.{collection} where date_uploaded=\"{date_uploaded}\";' >> ~/{file_name} && "
                + f"aws s3 cp ~/{file_name} s3://{published_bucket}/{s3_path}/"
                + f" &>> /var/log/kickstart_adg/e2e.log"
            )
            hive_export_list.append(hive_export_bash_command)

        return hive_export_list

    except BaseException as ex:
        console_printer.error_text_format(
            f"Test run failed while generating the data because of error - {str(ex)}"
        )
        sys.exit(-1)
