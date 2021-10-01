from helpers import aws_helper, console_printer
from typing import List, Dict


def aws_tags_are_subset(subset: List[Dict], superset: List[Dict],):
    subset_tags_flat = {tag["Key"]: tag["Value"] for tag in subset}
    superset_tags_flat = {tag["Key"]: tag["Value"] for tag in superset}

    for subset_key, subset_value in subset_tags_flat.items():
        if subset_key not in superset_tags_flat:
            return False
        if superset_tags_flat[subset_key] != subset_value:
            return False

    return True


def verify_s3_object_required_tags(s3_bucket, s3_object_key, tags_dict):
    # infer table/db from s3 object key
    console_printer.print_info(f"KEY: {s3_object_key}")
    split_string = s3_object_key.split("/")
    if len(split_string) < 3:
        console_printer.print_warning_text(
            f"Key doesn't match pattern: {s3_object_key}"
        )
        return

    # cater for keys ending "_$folder$"
    if split_string[-1].endswith("_$folder$"):
        split_string[-1] = split_string[-1][0:-9]

    # replace any strings ending `.db`
    if any(".db" in item for item in split_string):
        for index, value in enumerate(split_string):
            split_string[index] = value.replace(".db", "") if value.endswith(".db") else value

    db_name = None
    table_name = None

    # check if 2nd last string is in dict of DBs
    for i in range(2, 5):
        if split_string[-i] in tags_dict:
            db_name = split_string[-i]
            table_name = split_string[-(i-1)]
            break

    console_printer.print_info(f"DB: {db_name} | TABLE: {table_name}")
    if not (table_name and db_name):
        console_printer.print_warning_text(
            f"Couldn't identify db/table names from key: {s3_object_key}"
        )
        return

    if db_name not in tags_dict:
        console_printer.print_warning_text(
            f"db '{db_name}' not found in classification csv"
        )
        return
    elif table_name not in tags_dict[db_name]:
        console_printer.print_warning_text(
            f"table '{table_name}' not found in classification csv"
        )
        return

    else:
        required_tags_list = [
            {"Key": "table", "Value": table_name},
            {"Key": "db", "Value": db_name},
            {"Key": "pii", "Value": tags_dict[db_name][table_name]},
        ]

        aws_object_tags_list = aws_helper.get_tags_of_file_in_s3(
            s3_bucket=s3_bucket,
            s3_key=s3_object_key,
        ).get("TagSet")

        assert aws_tags_are_subset(
            subset=required_tags_list,
            superset=aws_object_tags_list,
        )


def get_rbac_csv_tags(rbac_csv_s3_bucket: str, rbac_csv_s3_key: str):
    rbac_csv = aws_helper.get_s3_object(s3_client=None, bucket=rbac_csv_s3_bucket, key=rbac_csv_s3_key)
    rows = [row.split(",") for row in rbac_csv.decode("utf-8").split("\r\n")]

    tags = {}
    for row in [row for row in rows[1:] if len(row) == 3]:
        if row[0] not in tags:
            tags[row[0]] = {}
        tags[row[0]][row[1]] = row[2]

    return tags
