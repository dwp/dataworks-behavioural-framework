import os
import json
import uuid
from datetime import datetime
from helpers import console_printer, template_helper, file_helper, date_helper


def generate_snapshot_file_from_hbase_records(
    test_run_name, topic, hbase_records_folder, output_folder
):
    """Generate raw snapshot file from hbase db object record for snapshot sender output comparisons.

    Keyword arguments:
    test_run_name -- unique name for this test run
    topic -- singular topic
    hbase_records_folder -- location for files containing a singular hbase record
    output_folder -- output folder for snapshot records
    """

    console_printer.print_info(
        f"Generating snapshot output for topic '{topic}' and folder '{hbase_records_folder}'"
    )

    hbase_records_folder_for_topic = os.path.join(hbase_records_folder, topic)

    snapshot_file_name = f"{topic}-snapshot-{test_run_name}.txt.gz.enc"
    hbase_records = file_helper.get_contents_of_files_in_folder(
        hbase_records_folder_for_topic, False
    )

    snapshot_file = os.path.join(output_folder, snapshot_file_name)

    with open(snapshot_file, "wt") as file_to_write:
        file_to_write.write("\n".join(hbase_records))

    return snapshot_file


def generate_hbase_record_for_snapshot_file(
    template_file_name,
    timestamp,
    record_id,
    record_type,
    test_run_name,
    topic,
    fixture_files_root,
    output_folder,
    is_adg=False,
):
    """Strip hbase record from snapshot file and put in temp folder

    Keyword arguments:
    template_file_name -- the name of the template to use
    timestamp -- the timestamp to use
    record_id -- the id to use
    record_type -- the type of record to use
    test_run_name -- unique name for this test run
    topic -- singular topic
    fixture_files_root -- parent folder with the fixture data in
    output_folder -- temp folder for snapshot generated files
    full_file_name -- full file name including the folder
    """
    console_printer.print_info(f"Checking base folder exists as '{output_folder}'")

    if not os.path.exists(output_folder):
        os.mkdir(output_folder)

    timestamp_string = date_helper.format_time_to_timezome_free(timestamp)
    epoch_timestamp = date_helper.generate_milliseconds_epoch_from_timestamp(
        timestamp_string[:-1]
    )

    console_printer.print_info(
        f"Generating snapshot record file for template of {template_file_name}, "
        + f"record type of '{record_type}', id of '{record_id}', "
        + f"fixture file root of '{fixture_files_root}', "
        + f"test run name of '{test_run_name}', "
        + f"epoch timestamp of '{str(epoch_timestamp)}' and timestamp of '{timestamp_string}'"
    )

    file_name = os.path.join(fixture_files_root, "snapshot_data", template_file_name)

    with open(file_name) as open_file:
        record = open_file.read()

    record = record.replace("||newid||", f"{record_id}")
    record = record.replace("||timestamp||", f"{timestamp_string}")
    record = record.replace("||epoch_timestamp||", f"{str(epoch_timestamp)}")
    record = record.replace("||record_type||", f"{record_type}")

    try:
        file_json = json.loads(record)
    except ValueError:
        console_printer.print_info(
            f"Could not load json from record contents of '{record}' and therefore no hbase record for snapshot file was generated"
        )
        return

    console_printer.print_info(f"Successfully loaded json contents from '{file_name}'")

    if "message" in file_json and "dbObject" in file_json["message"]:
        json_dumped = json.dumps(file_json["message"]["dbObject"])
    else:
        json_dumped = json.dumps(file_json)

    if is_adg:
        file_name = f"{topic}.txt.gz.enc"
    else:
        file_name = (
            f"{topic}-hbase-record-{test_run_name}-{str(uuid.uuid4())}.txt.gz.enc"
        )

    hbase_records_folder = os.path.join(output_folder, topic)
    hbase_record_file = os.path.join(hbase_records_folder, file_name)

    console_printer.print_info(
        f"Checking hbase records folder exists as '{hbase_records_folder}'"
    )

    if not os.path.exists(hbase_records_folder):
        os.mkdir(hbase_records_folder)

    console_printer.print_info(f"Writing hbase record file to '{hbase_record_file}'")

    with open(hbase_record_file, "w") as file_to_write:
        file_to_write.write(json_dumped)

    return hbase_record_file
