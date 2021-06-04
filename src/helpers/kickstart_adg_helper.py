import json
import csv
import os
import string
import uuid
import sys
import re
import random as rd
from datetime import datetime, timedelta
from helpers import (
    emr_step_generator,
    aws_helper,
    invoke_lambda,
    console_printer,
    file_helper,
    date_helper,
    historic_data_load_generator,
)


def dataGenBigint():
    return rd.choice([rd.getrandbits(32), None])


def dataGenBoolean():
    return rd.choice([True, False, None])


def dataGenSmallInt():
    return rd.choice([rd.getrandbits(4), None])


def dataGenNumeric():
    return rd.choice([round(rd.uniform(10000.0000, 99999.9999), 4), None])


def dataGenInteger():
    return rd.choice([rd.getrandbits(8), None])


def dataGenText():
    str_1 = "".join(rd.choice(string.ascii_letters + string.digits) for i in range(20))
    str_2 = "".join(rd.choice(" "))
    str_3 = "".join(rd.choice(string.digits + string.ascii_letters) for i in range(20))
    result_str = " ".join([str_1, str_2, str_3])
    return rd.choice([result_str, None])


def dataGenTimestamp():
    current_date = datetime.today()
    return rd.choice([datetime.strftime(current_date, "%Y-%m-%d %H:%M:%S"), None])


def dataGenUUID():
    return str(uuid.uuid1())


def dataGenString():
    letters = string.ascii_letters
    result_str = "".join(rd.choice(letters) for i in range(8))
    return rd.choice([result_str, None])


def dataGenDate():
    current_date = datetime.today()
    return rd.choice([datetime.strftime(current_date, "%Y-%m-%d 00:00:00")])


def dataGenDouble():
    return rd.choice([round(rd.random(), 2), None])

def dataGenUserInput():
    return rd.choice([
        """Camden will conduct an employability skills scan on each Kickstart participant at the point they join Camden.  This will create a skills profile which will allow us to see each individuals key areas of development and create a tailored training plan for them.\nEvery Kickstarter will receive a core suite of training that exceeds the requirements set out by the Kickstart scheme.  Core training includes a welcome week induction programme that aims to get Kickstarters invested in their own development and begin building the key skills and abilities that they will need throughout the programme.  The core training also includes a workshop on CV Writing and Jobsearch skills as well as a workshop on Preparing for and Being Successful in Interviews.\nEvery Kickstarter will be supported by Learning Mentor that will help them to keep an up to date learning journal and will guide them and their managers in selecting suitable training options that will create the best possible training programme for the individual.\n\nKickstarters will also be supported by our Camden Apprenticeships team that have lots of experience in supporting young people with their employability and career progression. team will provide pastoral support throughout the programme and will conduct an exit interview to review the Kickstarters career ambitions and outline their progression options.  Wherever possible Kickstarters will be progresses into a job or into an apprentices in the Council.  Where ongoing employment in the Council is not possible, the Kickstarters can choose to continue to receive support from Camden Apprenticeships to help find an apprenticeship and/or be linked up with other Councils other appropriate employment services such as Kings Cross Construction Skills Centre, Camden Job Hubs or Connexions Service.""",
        """Supported by \nAs a Gateway provider we have encouraged the organisations that we are representing to embrace this scheme to work with young people from difficult and challenging backgrounds as well as to uncover untapped talent regardless of race, age and gender.\nAs a kickstart Gateway and representing the individual company values, we want to make a difference to the lives of young people who face barriers to employment, and help them to overcome hardship through opportunities.\nWe are encouraging the organisations to provide training courses relative to the work that they will be doing, for example we are representing an accountancy organisation that is looking at taking a young person and they will be using part of their grant to invest in an entry level Zero/excel course for the young person\nWe are committed to supporting young people during and after the scheme, to help them find employment with another the organisation should the opportunity not arise in the existing company.\nThe initiative will enable us as a Gateway to build new positive relationships with Jobcentres and work coaches to ensure young people joining are identified in partnership with DWP.\nWe have created on line guidelines that form part of the young persons PDP covering communication, timekeeping, attendance and general guidance for the working environment\nAs part of our gateway commitment and responsibility we will be providing a Kickstart monthly blog on how the young person kickstart journey and developing and celebrating any achievements\n\n\nThe candidate will also get\nCV and LinkedIn Guidance\nInterview techniques\nMonthly Personal Development Plan\nWeekly Kickstart Updates""",
        """All Kickstart job placements are underpinned by an approach of helping, supporting, guiding, intervening, planning, progressing and achieving. We use motivational engagement methods in work with people who may have confidence issues, or other forms of barriers to progression. Our support staff assesses readiness to progressing in employment and identify and rectify any ambivalence or shortcomings at a pace works for the young person. We assess any barriers to employment with the job placement person to identify and implement a tailored support package using the activities described above and more. Each attender will receive a weekly supervision review gradually extended to match progression leading to a graduation point. All Kickstart attenders will develop an illustrated portfolio of their employability demonstrating more than a CV. The scheme manager will provide an end point assessment arrangement focused on achievements, capabilities and destination possibilities. We will ensure that every attender completing their work placement will enter a progression route."""
    ])

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
            "double": dataGenDouble,
            "user-input": dataGenText
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


def generate_csv_files(schema_config, local_output_folder, record_count):
    for collection, collection_schema in schema_config["schema"].items():
        run_date = datetime.strftime(datetime.now(), "%Y-%m-%d")
        epoc_time = str(date_helper.get_current_epoch_seconds())

        for keys,item in schema_config["output_file_pattern"].items():
            for num in range(1, item["total_files"]):
                output_file_name = (
                    item["file_pattern"]
                    .replace("run-date", run_date)
                    .replace("collection", collection)
                    .replace("epoc-time", epoc_time)
                    .replace("seq-num", str(seq_num))
                )

                output_file = os.path.join(local_output_folder, output_file_name)

                console_printer.print_info(
                    f"opening the file {output_file} to write test data"
                )
                with open(output_file, "w+", newline='') as csvfile:
                    writer = csv.writer(csvfile, delimiter=schema_config['record_delimiter'])
                    header_record = collection_schema.keys()
                    writer.writerow(header_record)
                    num = 1
                    while num <= int(record_count):
                        record_data = [
                            str(dataTypeMapping(type)())
                            for key, type in collection_schema.items()
                        ]
                        writer.writerow(record_data)
                        num += 1

def generate_json_files(schema_config, local_output_folder, record_count):
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
        num = 1
        data = []
        JSON_BLOB = {
            "extract": {
                "service": "application-service",
                "dataExtract": "application",
                "start": datetime.strftime(
                    datetime.now() - timedelta(days=1), "%Y-%m-%dT%H:%M:%SZ"
                ),
                "end": datetime.strftime(datetime.now(), "%Y-%m-%dT%H:%M:%SZ"),
            },
            "fields": [
                {"fieldName": column, "pii": column_property["pii_flg"]}
                for column, column_property in collection_schema.items()
            ],
        }
        with open(output_file, "w+") as writer:
            while num <= int(record_count):
                record = {}
                for column, column_property in collection_schema.items():
                    record.update(
                        {column: dataTypeMapping(column_property["value"])()}
                    )
                    if "default" in column_property:
                        record.update(
                            {column: rd.choice(column_property["default"])}
                        )
                data.append(record)
                num += 1
            JSON_BLOB.update({"data": data})
            writer.write(json.dumps(JSON_BLOB, indent=4))

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
            generate_csv_files(schema_config, local_output_folder, record_count)

        elif schema_config["record_layout"].lower() == "json":
            generate_json_files(schema_config, local_output_folder, record_count)

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
        if schema_config["record_layout"].lower() == "csv":
            for collection, collections_schema in schema_config["schema"].items():
                date_uploaded = datetime.strftime(datetime.now(), "%Y-%m-%d")
                file_name = f"e2e_{collection}.csv"
                column_name = ",".join(
                    [
                        re.sub("[^0-9a-zA-Z$]+", " ", col)
                        .strip()
                        .replace(" ", "_")
                        .lower()
                        for col in collections_schema.keys()
                    ]
                )
                hive_export_bash_command = (
                    f"hive -e 'SELECT {column_name} FROM uc_kickstart.{collection} where date_uploaded=\"{date_uploaded}\";' >> ~/{file_name} && "
                    + f"aws s3 cp ~/{file_name} s3://{published_bucket}/{s3_path}/"
                    + f" &>> /var/log/kickstart_adg/e2e.log"
                )
                hive_export_list.append(hive_export_bash_command)

        elif schema_config["record_layout"].lower() == "json":
            for collection, collections_schema in schema_config["schema"].items():
                date_uploaded = datetime.strftime(datetime.now(), "%Y-%m-%d")
                file_name = f"e2e_{collection}.csv"
                column_name = ",".join(
                    [
                        col[0].lower()
                        + re.sub(
                            r"(?!^)[A-Z]", lambda x: "_" + x.group(0).lower(), col[1:]
                        )
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


def files_upload_to_s3(context, local_file_list, folder_name, upload_method):

    for file in local_file_list:
        if upload_method.lower() == "unencrypted":
            console_printer.print_info(
                f"Data will be uploaded in {upload_method} format to s3 bucket"
            )
            console_printer.print_info(f"The file name is {file}")
            file_name = os.path.basename(file)
            input_file = file_helper.get_contents_of_file(file, False)
            inputs_s3_key = os.path.join(folder_name, file_name)
            console_printer.print_info(
                f"Uploading the local file {file} with basename as {file_name} into s3 bucket {context.published_bucket} using key name as {inputs_s3_key}"
            )
            aws_helper.put_object_in_s3(
                input_file, context.published_bucket, inputs_s3_key
            )
        elif upload_method.lower() == "encrypted":
            console_printer.print_info(
                f"Data will be uploaded in {upload_method} format to s3 bucket"
            )
            console_printer.print_info(f"The input file name is {file}")

            file_name = os.path.basename(file)
            encrypted_key = context.encryption_encrypted_key
            master_key = context.encryption_master_key_id
            plaintext_key = context.encryption_plaintext_key
            [
                file_iv_int,
                file_iv_whole,
            ] = historic_data_load_generator.generate_initialisation_vector()

            console_printer.print_info(f"Extracting the raw data from local directory")
            data = file_helper.get_contents_of_file(file, False).encode("utf-8")

            console_printer.print_info(f"Applying encryption to the raw data")
            input_data = historic_data_load_generator.encrypt(
                file_iv_whole, plaintext_key, data
            )
            inputs_s3_key = os.path.join(folder_name, file_name + ".enc")

            all_metadata = json.loads(
                historic_data_load_generator.generate_encryption_metadata_for_metadata_file(
                    encrypted_key, master_key, plaintext_key, file_iv_int
                )
            )

            console_printer.print_info("Metadata of for encrypted file is \n")
            console_printer.print_info(f"{json.dumps(all_metadata)}")

            metadata = {
                "iv": all_metadata["initialisationVector"],
                "ciphertext": all_metadata["encryptedEncryptionKey"],
                "datakeyencryptionkeyid": all_metadata["keyEncryptionKeyId"],
            }
            console_printer.print_info(
                f"Uploading the local file {file} with basename as {file_name} into s3 bucket {context.published_bucket} using key name as {inputs_s3_key} and along with metadata"
            )

            aws_helper.put_object_in_s3_with_metadata(
                input_data, context.published_bucket, inputs_s3_key, metadata=metadata
            )
