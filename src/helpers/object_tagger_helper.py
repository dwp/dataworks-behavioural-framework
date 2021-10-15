import logging

from helpers import aws_helper, console_printer
from typing import List, Dict


def aws_tags_are_subset(
    subset: List[Dict],
    superset: List[Dict],
):
    subset_tags_flat = {tag["Key"]: tag["Value"] for tag in subset}
    superset_tags_flat = {tag["Key"]: tag["Value"] for tag in superset}

    for subset_key, subset_value in subset_tags_flat.items():
        if subset_key not in superset_tags_flat:
            return False
        if superset_tags_flat[subset_key] != subset_value:
            return False

    return True


def rbac_required_tags(s3_object_key, tags_dict):
    # infer table/db from s3 object key
    split_string = s3_object_key.split("/")
    if len(split_string) < 3:
        console_printer.print_warning_text(
            f"Key doesn't match pattern: {s3_object_key}"
        )
        return []

    # replace keys ending "_$folder$"
    if split_string[-1].endswith("_$folder$"):
        split_string[-1] = split_string[-1][0:-9]

    # replace any substrings ending ".db"
    for index, value in enumerate(split_string):
        split_string[index] = (
            value.replace(".db", "") if value.endswith(".db") else value
        )

    db_name = None
    table_name = None

    # check strings backwards from second-last component for match in rbac csv
    try:
        for i in range(2, 5):
            if split_string[-i] in tags_dict:
                db_name = split_string[-i]
                table_name = split_string[-(i - 1)]
                break
    except Exception as e:
        logging.warning(f"Caught exception while inferring db name: {e}")
        return []

    if not (table_name and db_name):
        console_printer.print_warning_text(
            f"Couldn't identify db/table names from key: {s3_object_key}"
        )
        return []

    elif table_name not in tags_dict[db_name]:
        console_printer.print_warning_text(
            f"table '{table_name}' not found in classification csv"
        )
        return [
            {"Key": "table", "Value": table_name},
            {"Key": "db", "Value": db_name},
            {"Key": "pii", "Value": ""},
        ]

    else:
        return [
            {"Key": "table", "Value": table_name},
            {"Key": "db", "Value": db_name},
            {"Key": "pii", "Value": tags_dict[db_name][table_name]},
        ]


def get_rbac_csv_tags(rbac_csv_s3_bucket: str, rbac_csv_s3_key: str):
    rbac_csv = aws_helper.get_s3_object(
        s3_client=None, bucket=rbac_csv_s3_bucket, key=rbac_csv_s3_key
    )
    rows = [row.split(",") for row in rbac_csv.decode("utf-8").split("\r\n")]

    tags = {}
    for row in [row for row in rows[1:] if len(row) == 3]:
        if row[0] not in tags:
            tags[row[0]] = {}
        tags[row[0]][row[1]] = row[2]

    return tags
