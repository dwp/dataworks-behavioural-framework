import base64
import decimal
import json
import os
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, wait
from functools import reduce
from typing import List

import boto3
from boto3.dynamodb.conditions import Key, And
from boto3.exceptions import S3UploadFailedError
from botocore.config import Config
from botocore.exceptions import ClientError

from helpers import invoke_lambda, template_helper, console_printer

aws_role_arn = None
aws_profile = None
aws_session_timeout_seconds = None
boto3_session = None

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        if isinstance(o, set):  # <---resolving sets as lists
            return list(o)
        return super(DecimalEncoder, self).default(o)


def set_details_for_role_assumption(role, session_timeout_seconds):
    """Sets the global aws details for role assumption to use for boto3 sessions.

    Keyword arguments:
    role -- the role arn
    session_timeout_seconds -- the session timeout in seconds
    """
    global aws_role_arn
    global aws_session_timeout_seconds

    aws_role_arn = role
    aws_session_timeout_seconds = session_timeout_seconds

    console_printer.print_info(f"Set aws role to use to '{aws_role_arn}'")


def clear_session():
    """Clears the current session, ensuring that the next call creates a new one, useful for long running processes."""
    global boto3_session

    boto3_session = None

    console_printer.print_info(
        "Cleared AWS session, new one will be created at next service or resource request"
    )


def get_session_credentials():
    """Retrieves the credentials for the current session (creates one if not there)."""
    global boto3_session

    ensure_session()

    return boto3_session.get_credentials()


def ensure_session(profile=None):
    """Ensures the global boto3 session is created.

    Keyword arguments:
    profile -- the profile name to use (if None, default profile is used)
    """
    global aws_role
    global aws_profile
    global boto3_session

    if boto3_session is None or aws_profile != profile:
        aws_profile = profile

        # Initial sessions using default creds to assume the role
        boto3_session = boto3.session.Session()
        credentials_dict = assume_role()

        if profile is None:
            boto3_session = boto3.session.Session(
                aws_access_key_id=credentials_dict["AccessKeyId"],
                aws_secret_access_key=credentials_dict["SecretAccessKey"],
                aws_session_token=credentials_dict["SessionToken"],
            )
            console_printer.print_info(f"Creating new aws session with no profile")
        else:
            boto3_session = boto3.session.Session(
                aws_access_key_id=credentials_dict["AccessKeyId"],
                aws_secret_access_key=credentials_dict["SecretAccessKey"],
                aws_session_token=credentials_dict["SessionToken"],
                profile_name=profile,
            )
            console_printer.print_info(
                f"Creating new aws session with profile of '{aws_profile}'"
            )


def assume_role():
    """Assumes the role needed for the boto3 session.

    Keyword arguments:
    profile -- the profile name to use (if None, default profile is used)
    """
    global aws_role_arn
    global aws_session_timeout_seconds
    global boto3_session

    if aws_role_arn is None or aws_session_timeout_seconds is None:
        raise AssertionError(
            f"AWS role arn ('{aws_role_arn}') or session timeout in seconds ('{aws_session_timeout_seconds}') not set, ensure 'set_details_for_role_assumption()' has been run"
        )

    session_name = "e2e_test_run_" + str(uuid.uuid4())
    console_printer.print_info(
        f"Assuming aws role via sts with arn of '{aws_role_arn}', timeout of '{aws_session_timeout_seconds}' and session name of '{session_name}'"
    )

    sts_client = boto3_session.client("sts")
    assume_role_dict = sts_client.assume_role(
        RoleArn=aws_role_arn,
        RoleSessionName=f"{session_name}",
        DurationSeconds=int(aws_session_timeout_seconds),
    )

    return assume_role_dict["Credentials"]


def get_client(service_name, profile=None, region=None, read_timeout_seconds=120):
    """Creates a standardised boto3 client for the given service.

    Keyword arguments:
    service_name -- the aws service name (i.e. s3, lambda etc)
    profile -- the profile to use (defaults to no profile)
    region -- use a specific region for the client (defaults to no region with None)
    read_timeout -- timeout for operations, defaults to 120 seconds
    """
    global boto3_session

    ensure_session(profile)

    max_connections = 25 if service_name.lower() == "s3" else 10

    client_config = Config(
        read_timeout=read_timeout_seconds,
        max_pool_connections=max_connections,
        retries={"max_attempts": 10, "mode": "standard"},
    )

    if region is None:
        return boto3_session.client(service_name, config=client_config)
    else:
        return boto3_session.client(
            service_name, region_name=region, config=client_config
        )


def get_resource(resource_name, profile=None):
    """Creates a standardised boto3 resource service client for the given resource name.

    Keyword arguments:
    resource_name -- the aws resource name (i.e. s3, lambda etc)
    profile -- the profile to use (defaults to no profile)
    """
    global boto3_session

    ensure_session(profile)

    return boto3_session.resource(resource_name)


def get_item_from_dynamodb(table_name, key_dict):
    """Returns the given item from dynamodb.

    Keyword arguments:
    table_name -- the name for the table in dynamodb
    key_dict -- dictionary of key/value pairs for the item key
    """
    dynamodb_client = get_client(service_name="dynamodb")

    console_printer.print_debug(
        f"Getting DynamoDb data from item with key_dict of '{key_dict}' from table named '{table_name}'"
    )

    return dynamodb_client.get_item(TableName=table_name, Key=key_dict)


def scan_dynamodb_with_filters(table_name, filters):
    """Returns the ga scan of the dynamodb table.

    Keyword arguments:
    table_name -- the name for the table in dynamodb
    filters -- a dictionary of keys and their values to filter the table by
    """
    dynamodb = get_resource(resource_name="dynamodb")

    console_printer.print_debug(
        f"Getting DynamoDb data from table named '{table_name}'"
    )
    table = dynamodb.Table(table_name)
    response = table.scan(
        FilterExpression=reduce(And, ([Key(k).eq(v) for k, v in filters.items()]))
    )
    # Dynamo returns integers with the word Decimal around them. The below sorts this out
    clean_response = json.dumps((response["Items"]), indent=4, cls=DecimalEncoder)

    return json.loads(clean_response)


def get_item_from_dynamodb(table_name, key_dict):
    """Gets the given item from dynamodb.

    Keyword arguments:
    table_name -- the name for the table in dynamodb
    key_dict -- dictionary of key/value pairs for the item key
    """
    dynamodb_client = get_client(service_name="dynamodb")

    return dynamodb_client.get_item(TableName=f"{table_name}", Key=key_dict)


def delete_item_from_dynamodb(table_name, key_dict):
    """Deletes the given item from dynamodb.

    Keyword arguments:
    table_name -- the name for the table in dynamodb
    key_dict -- dictionary of key/value pairs for the item key
    """
    dynamodb_client = get_client(service_name="dynamodb")

    console_printer.print_info(
        f"Deleting DynamoDb item with key_dict of '{key_dict}' from table named '{table_name}'"
    )

    return dynamodb_client.delete_item(TableName=f"{table_name}", Key=key_dict)


def insert_item_to_dynamo_db(table_name, item_dict):
    """Puts an item in to a table in dynamodb.

    Keyword arguments:
    table_name -- the name for the table in dynamodb
    item_dict -- dictionary of key/value pairs for the item
    """
    dynamodb_client = get_client(service_name="dynamodb")

    console_printer.print_info(
        f"Putting DynamoDb item with dict of '{item_dict}' in to table named {table_name}"
    )

    dynamodb_client.put_item(TableName=f"{table_name}", Item=item_dict)


def update_item_in_dynamo_db(
    table_name, key_dict, update_expression, expression_attribute_values_dict
):
    """Updates an item in a table in dynamodb (or creates if not exists).

    Keyword arguments:
    table_name -- the name for the table in dynamodb
    key_dict -- dictionary of key/value pairs for the item key
    update_expression -- the expression for what to update
    expression_attribute_values_dict -- the key/value pairs of what to update
    """
    dynamodb_client = get_client(service_name="dynamodb")

    console_printer.print_info(
        f"Updating DynamoDb item with dict of '{key_dict}', "
        + f"update expression of '{update_expression}' and attribute values dict of '{expression_attribute_values_dict}' in table named {table_name}"
    )

    dynamodb_client.update_item(
        TableName=f"{table_name}",
        Key=key_dict,
        UpdateExpression=f"{update_expression}",
        ExpressionAttributeValues=expression_attribute_values_dict,
    )


def retrieve_data_from_hbase(id_object, topic_name, wrap_id=False):
    """Retrieves data from HBase in a decoded format.

    Keyword arguments:
    id_object -- the json id object
    topic_name -- the topic name for the data in hbase
    wrap_id -- True is the id format should be wrapped with an "id" object
    """
    id_string = json.dumps(id_object).strip('"')

    if wrap_id:
        id_string = json.dumps({"id": id_string})

    payload_dict = {"topic": topic_name, "key": id_string}
    payload_json = json.dumps(payload_dict)

    hbase_data = invoke_lambda.invoke_hbase_retriever(payload_json)

    if hbase_data is not None and hbase_data != "null":
        try:
            decoded_json = base64.b64decode(hbase_data).decode("utf-8")
            return decoded_json.replace("\n", "").replace(" ", "")
        except:
            console_printer.print_info(
                f"Could not decode returned hbase data of '{hbase_data}'"
            )

    return None


def resize_auto_scaling_group(service, size):
    """Calls the lambda which scales auto-scaling groups

    Keyword arguments:
    service -- the service to scale, i.e. "htme_" or "snapshot_sender_"
    size -- the size as a string to scale to
    """
    message = '{{"asg_prefix": "{0}","asg_size": "{1}"}}'.format(service, size)
    console_printer.print_bold_text(f"Scaling ASG: {message}")

    payload_dict = {"Records": [{"Sns": {"Message": message}}]}
    payload_json = json.dumps(payload_dict)
    return invoke_lambda.invoke_asg_resizer(payload_json)


def retrieve_files_from_s3(s3_bucket, path, pattern=None, remove_whitespace=False):
    """Retrieves data from S3 files in a any format.

    Keyword arguments:
    s3_bucket -- the S3 bucket name
    path -- the path from the root bucket to the files
    pattern -- pattern to match the full key against (optional, if None finds all objects)
    """
    files = []
    s3_client = get_client(service_name="s3")

    console_printer.print_info(
        f"Getting for all files in '{path}' in '{s3_bucket}' bucket using pattern '{pattern}'"
    )

    objects = s3_client.list_objects(Bucket=s3_bucket, Prefix=path)

    if "Contents" in objects:
        for single_object in objects["Contents"]:
            if (
                pattern is None
                or re.fullmatch(pattern, single_object["Key"], flags=0) is not None
            ):
                s3_object = s3_client.get_object(
                    Bucket=s3_bucket, Key=single_object["Key"]
                )
                data = s3_object["Body"].read().decode()
                if remove_whitespace:
                    data = data.replace("\n", "").replace(" ", "")
                files.append(data)

    return files


def clear_s3_prefix(s3_bucket, path, delete_prefix, path_is_folder=True):
    """Clears a given S3 prefix and waits for the clearance to complete.

    Keyword arguments:
    s3_bucket -- the S3 bucket name
    path -- the path from the root bucket to the files
    delete_prefix -- True to delete the specific prefix itself after clearing underneath it
    path_is_folder -- true to ensure the path is treated like a folder
    """
    console_printer.print_info(
        f"Clearing S3 prefix of '{path}' in '{s3_bucket}' bucket"
    )

    if does_s3_key_exist(s3_bucket, path):
        s3 = get_resource(resource_name="s3")
        qualified_path = os.path.join(path, "") if path_is_folder else path
        bucket = s3.Bucket(s3_bucket)
        for bucket_object in bucket.objects.filter(Prefix=qualified_path):
            if delete_prefix or qualified_path != bucket_object.key:
                bucket_object.delete()
                bucket_object.wait_until_not_exists()


def get_total_size_in_bytes_of_s3_folder(s3_bucket, path):
    """Returns the total size of the items with the given prefix.

    Keyword arguments:
    s3_bucket -- the S3 bucket name
    path -- the path from the root bucket to the files
    """
    total_size = 0
    if does_s3_key_exist(s3_bucket, path):
        s3 = get_resource(resource_name="s3")
        qualified_path = os.path.join(path, "")
        bucket = s3.Bucket(s3_bucket)
        for bucket_object in bucket.objects.filter(Prefix=qualified_path):
            total_size += bucket_object.size

    return total_size


def truncate_hbase_table(topic_name):
    """Clears all messages from hbase table for a given topic.

    Keyword arguments:
    topic_name -- Name of the topic
    """
    # 'deleteEntireTableWhenInDeleteMode' must be False when 'deleteRequest' is True here due to us wanting to truncate table not delete it
    # Table per topic on delete mode would delete the table
    payload_dict = {
        "topic": topic_name,
        "deleteRequest": True,
        "deleteEntireTableWhenInDeleteMode": False,
    }
    payload_json = json.dumps(payload_dict)

    console_printer.print_info(f"Deleting data in HBase with payload {payload_json}")
    invoke_lambda.invoke_hbase_retriever(payload_json)

    return topic_name


def assert_snapshots_in_s3_threaded(
    topics, s3_bucket, s3_prefix, expected_file_count, timeout, exact_count_match=True
):
    """Returns the body of any snapshots found for the topics using threads.

    Keyword arguments:
    topics -- the topic names to use
    s3_bucket -- the S3 bucket name
    s3_prefix -- the fixed S3 prefix to use without wildcards
    expected_file_count -- the number of expected files as an int
    timeout -- timeout in seconds
    exact_count_match -- if False does a greater than match on the count
    """
    s3_client = get_client("s3")

    with ThreadPoolExecutor() as executor:
        future_results = []

        for topic in topics:
            pattern = f"^{s3_prefix}/{topic}-\d{{3}}-\d{{3}}-\d+.txt.gz.enc$"
            future_results.append(
                executor.submit(
                    assert_files_in_s3_matching_pattern,
                    s3_bucket,
                    s3_prefix,
                    pattern,
                    expected_file_count,
                    timeout,
                    exact_count_match,
                    s3_client,
                )
            )

        wait(future_results)
        for future in future_results:
            try:
                yield future.result()
            except Exception as ex:
                raise AssertionError(ex)


def assert_no_snapshots_in_s3_threaded(topics, s3_bucket, s3_prefix, timeout):
    """Ensures no snapshots found for the topics using threads.

    Keyword arguments:
    topics -- the topic names to use
    s3_bucket -- the S3 bucket name
    s3_prefix -- the fixed S3 prefix to use without wildcards
    timeout -- timeout in seconds
    """
    s3_client = get_client("s3")

    with ThreadPoolExecutor() as executor:
        future_results = []

        for topic in topics:
            pattern = f"^{s3_prefix}/{topic}-\d{{3}}-\d{{3}}-\d+.txt.gz.enc$"
            future_results.append(
                executor.submit(
                    assert_no_files_in_s3_matching_pattern,
                    s3_bucket,
                    s3_prefix,
                    pattern,
                    timeout,
                    s3_client,
                )
            )

        wait(future_results)
        for future in future_results:
            try:
                yield future.result()
            except Exception as ex:
                raise AssertionError(ex)


def assert_no_files_in_s3_matching_pattern(
    s3_bucket, s3_prefix, pattern, timeout, s3_client=None
):
    """Asserts no objects found matching the pattern

    Keyword arguments:
    s3_bucket -- the S3 bucket name
    s3_prefix -- the fixed S3 prefix to use without wildcards
    pattern -- pattern to match the full key against
    timeout -- timeout in seconds
    s3_client -- client to override the standard one
    """
    console_printer.print_info(
        f"Asserting no files in '{s3_prefix}' in '{s3_bucket}' bucket using pattern '{pattern}'"
    )

    if s3_client is None:
        s3_client = get_client("s3")

    timeout_time = time.time() + timeout
    files_found = False
    while time.time() < timeout_time and files_found == False:
        keys = get_s3_file_object_keys_matching_pattern(
            s3_bucket, s3_prefix, pattern, s3_client
        )
        if len(keys) > 0:
            files_found = True
        time.sleep(1)

    if files_found:
        raise AssertionError(
            f"Query returns exported files in S3 bucket within {str(timeout)} seconds"
        )

    return s3_prefix


def assert_files_in_s3_matching_pattern(
    s3_bucket,
    s3_prefix,
    pattern,
    expected_file_count,
    timeout,
    exact_count_match=True,
    s3_client=None,
):
    """Returns the body of any objects found matching the pattern

    Keyword arguments:
    s3_bucket -- the S3 bucket name
    s3_prefix -- the fixed S3 prefix to use without wildcards
    pattern -- pattern to match the full key against
    expected_file_count -- the number of expected files as an int
    timeout -- timeout in seconds
    exact_count_match -- if False does a greater than match on the count
    s3_client -- client to override the standard one
    """
    console_printer.print_info(
        f"Asserting '{expected_file_count}' files in '{s3_prefix}' in '{s3_bucket}' bucket using pattern '{pattern}'"
    )

    if s3_client is None:
        s3_client = get_client("s3")

    timeout_time = time.time() + timeout
    results_valid = False
    while time.time() < timeout_time and results_valid == False:
        keys = get_s3_file_object_keys_matching_pattern(
            s3_bucket, s3_prefix, pattern, s3_client
        )
        if (exact_count_match and len(keys) == expected_file_count) or len(
            keys
        ) >= expected_file_count:
            results_valid = True
        time.sleep(1)

    if not results_valid:
        raise AssertionError(
            f"Query returns no exported files in S3 bucket within {str(timeout)} seconds"
        )

    return s3_prefix


def get_s3_file_object_keys_matching_pattern(
    s3_bucket, s3_prefix, pattern=None, s3_client=None
):
    """Returns the keys of any objects found matching the pattern

    Keyword arguments:
    s3_bucket -- the S3 bucket name
    s3_prefix -- the fixed S3 prefix to use without wildcards
    pattern -- pattern to match the full key against
    s3_client -- client to override the standard one
    """
    keys = []

    if s3_client is None:
        s3_client = get_client(service_name="s3")

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix)

    for page in pages:
        if "Contents" in page:
            for obj in page["Contents"]:
                key = obj["Key"]
                if pattern is None or re.fullmatch(pattern, key, flags=0) is not None:
                    keys.append(key)

    return keys


def does_s3_key_exist(s3_bucket, key):
    """Returns true if the given key exists in the bucket, false otherwise.

    Keyword arguments:
    s3_bucket -- the S3 bucket name
    key -- the key to look for, could be a file path and key or simply a path
    """
    s3_client = get_client(service_name="s3")

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=s3_bucket, Prefix=key)

    key_count = 0
    for page in pages:
        key_count += page["KeyCount"]

    return key_count > 0


def get_number_of_s3_objects_for_key(s3_bucket, key):
    """Returns the number of objects with the given key in the bucket.

    Keyword arguments:
    s3_bucket -- the S3 bucket name
    key -- the key to look for, could be a file path and key or simply a path
    """
    s3_client = get_client(service_name="s3")

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=s3_bucket, Prefix=key)

    key_count = 0
    for page in pages:
        key_count += page["KeyCount"]

    return key_count


def upload_file_to_s3_and_wait_for_consistency_threaded(
    input_folder, s3_bucket, seconds_timeout, s3_prefix
):
    """Uploads the given file to s3 and waits for the file to be written, will error if not consistent after 6 minutes using threads.

    Keyword arguments:
    input_folder -- the folder with the files to upload in
    s3_bucket -- the bucket to upload to in s3
    seconds_timeout -- the number of seconds to wait for
    s3_prefix -- the prefix of where to put the files
    """
    s3_client = get_client(service_name="s3")

    with ThreadPoolExecutor() as executor:
        future_results = []

        for output_file in os.listdir(input_folder):
            future_results.append(
                executor.submit(
                    upload_file_to_s3_and_wait_for_consistency,
                    os.path.join(input_folder, output_file),
                    s3_bucket,
                    seconds_timeout,
                    os.path.join(s3_prefix, output_file),
                    s3_client,
                )
            )

        wait(future_results)
        for future in future_results:
            try:
                yield future.result()
            except Exception as ex:
                raise AssertionError(ex)


def upload_directory_to_s3(input_folder, s3_bucket, seconds_timeout, s3_prefix):
    """Uploads the given file to s3 and waits for the file to be written, will error if not consistent after 6 minutes using threads.

    Keyword arguments:
    input_folder -- the folder with the files to upload in
    s3_bucket -- the bucket to upload to in s3
    seconds_timeout -- the number of seconds to wait for
    s3_prefix -- the prefix of where to put the files
    """
    s3_client = get_client(service_name="s3")

    for root, dirs, files in os.walk(input_folder):
        for dir_name in dirs:
            full_dir = os.path.join(root, dir_name)
            relative_dir = full_dir.split(input_folder)[-1].lstrip("/")
            full_s3_prefix = os.path.join(s3_prefix, relative_dir)
            for output_file in os.listdir(full_dir):
                full_local_file = os.path.join(full_dir, output_file)
                if not os.path.isdir(full_local_file):
                    full_s3_file = os.path.join(full_s3_prefix, output_file)
                    upload_file_to_s3_and_wait_for_consistency(
                        full_local_file, s3_bucket, seconds_timeout, full_s3_file
                    )


def upload_file_to_s3_and_wait_for_consistency(
    file_location, s3_bucket, seconds_timeout, s3_key, s3_client=None
):
    """Uploads the given file to s3 and waits for the file to be written, will error if not consistent after 6 minutes.

    Keyword arguments:
    file_location -- the local path and name of the file to upload
    s3_bucket -- the bucket to upload to in s3
    seconds_timeout -- the number of seconds to wait for
    s3_key -- the key of where to upload the file to in s3
    s3_client -- client to override the standard one
    """
    if s3_client is None:
        s3_client = get_client(service_name="s3")

    s3_client.upload_file(file_location, s3_bucket, s3_key)

    return wait_for_file_to_be_in_s3(s3_bucket, s3_key, seconds_timeout, s3_client)


def remove_file_from_s3_and_wait_for_consistency(s3_bucket, s3_key, s3_client=None):
    """Removes file and waits for consistency, will error if not consistent after 2 minutes.

    Keyword arguments:
    s3_bucket -- the bucket with the file/object - s3
    s3_key -- the key of where to find the file/object in s3
    s3_client -- client to override the standard one
    """
    if s3_client is None:
        s3_client = get_client(service_name="s3")

    s3_client.delete_object(Bucket=s3_bucket, Key=s3_key)
    return wait_for_file_to_be_removed_from_s3(s3_bucket, s3_key, s3_client)


def wait_for_file_to_be_removed_from_s3(s3_bucket, s3_key, s3_client=None):
    """Waits for the file to be removed from s3, will error if not consistent after 2 minutes.

    Keyword arguments:
    s3_bucket -- the bucket with the file/object - s3
    s3_key -- the key of where to find the file/object in s3
    s3_client -- client to override the standard one
    """
    if s3_client is None:
        s3_client = get_client(service_name="s3")

    waiter = s3_client.get_waiter("object_not_exists")
    waiter.wait(
        Bucket=s3_bucket, Key=s3_key, WaiterConfig={"Delay": 2, "MaxAttempts": 60}
    )
    return s3_key


def wait_for_file_to_be_in_s3(s3_bucket, s3_key, seconds_timeout, s3_client=None):
    """Waits for the file to be in s3, will error if not consistent after 2 minutes.

    Keyword arguments:
    s3_bucket -- the bucket to upload to in s3
    s3_key -- the key of where to upload the file to in s3
    seconds_timeout -- the number of seconds to wait for
    s3_client -- client to override the standard one
    """
    if s3_client is None:
        s3_client = get_client(service_name="s3")

    waiter = s3_client.get_waiter("object_exists")
    waiter.wait(
        Bucket=s3_bucket,
        Key=s3_key,
        WaiterConfig={"Delay": 1, "MaxAttempts": seconds_timeout},
    )
    return s3_key


def add_tags_to_file_in_s3(s3_bucket, s3_key, tagging_object_array, s3_client=None):
    """Adds tags to existing files in s3.

    Keyword arguments:
    s3_bucket -- the bucket in which the files is stored
    s3_key -- the pathe to the file in s3
    tagging_object_array -- an object with tags in it ie.:
            [{
                'Key': 'tag-key',
                'Value': 'tag-value'
            },]
    s3_client -- client to override the standard one
    """
    if s3_client is None:
        s3_client = get_client(service_name="s3")

    s3_client.put_object_tagging(
        Bucket=s3_bucket,
        Key=s3_key,
        Tagging={
            "TagSet": tagging_object_array,
        },
    )


def get_tags_of_file_in_s3(s3_bucket, s3_key, s3_client=None):
    """Retrieves tags of the given key in s3.

    Keyword arguments:
    s3_bucket -- the bucket in which the files is stored
    s3_key -- the pathe to the file in s3
    s3_client -- client to override the standard one
    """
    if s3_client is None:
        s3_client = get_client(service_name="s3")

    return s3_client.get_object_tagging(Bucket=s3_bucket, Key=s3_key)


def put_object_in_s3_with_metadata(body, s3_bucket, s3_key, metadata):
    """Adds metadata to existing files in s3.

    Keyword arguments:
    s3_bucket -- the bucket in which the files is stored
    s3_key -- the path to the file in s3
    metadata -- an object with metadata key values  in it ie.:
            [{
                'Key': 'key',
                'Value': 'value'
            },]
    s3_client -- client to override the standard one
    """
    s3_client = get_client(service_name="s3")
    s3_client.put_object(Body=body, Bucket=s3_bucket, Key=s3_key, Metadata=metadata)


def put_object_in_s3(body, s3_bucket, s3_key):
    """Adds files to s3.

    Keyword arguments:
    s3_bucket -- the bucket in which the files is stored
    s3_key -- the pathe to the file in s3
    s3_client -- client to override the standard one
    """
    s3_client = get_client(service_name="s3")
    s3_client.put_object(Body=body, Bucket=s3_bucket, Key=s3_key)


def send_message_to_sqs(queue_url, message_body, message_group_id=None):
    """Post a message to an SQS queue.

    Keyword arguments:
    queue_url -- the url of the sqs queue to send the message to
    message_body -- the string body for the message
    message_group_id -- if not None, then uses this message group
    """
    console_printer.print_info(
        f"Sending message to sqs: '{message_body}' on queue '{queue_url}'"
    )
    sqs_client = get_client(service_name="sqs")

    if message_group_id is not None:
        sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=message_body,
            MessageGroupId=message_group_id,
        )
    else:
        sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=message_body,
        )


def send_files_to_kafka_producer_sns(
    dynamodb_table_name,
    s3_input_bucket,
    aws_acc_id,
    sns_topic_name,
    fixture_files,
    message_key,
    topic_name,
    topic_prefix,
    region,
    skip_encryption="false",
    kafka_message_volume="1",
    kafka_random_key="false",
    wait_for_job_completion=True,
):
    """Post an SNS message to the topic for a single file that then sends it to the Kafka broker.

    Keyword arguments:
    dynamodb_table_name -- the name of the dynamodb table to use
    s3_input_bucket -- the name of the s3 bucket containing the fixture files
    aws_acc_id -- the account id for the aws account in use
    sns_topic_name -- the topic name used by sns to send the message to
    topic_name -- the name for the topic to add to the prefix
    context -- the behave context object
    fixture_files -- a list of fixture data files to post to SNS
    message_key -- the key used for the message which will form the Kafka message key as a string
    skip_encryption -- a valid true string to skip encryption of the dbObject field
    wait_for_job_completion -- bool flag to indicate if dynamodb job status should be checked
    topic_prefix -- the prefix for the topics
    region -- the AWS region
    """
    message_key = str(message_key).strip('"')
    message_key = message_key.strip('"')

    arn_suffix = f"{aws_acc_id}:{sns_topic_name}"
    arn_value = generate_arn("sns", arn_suffix, region)

    message = {
        "job_id": template_helper.get_short_topic_name(topic_name),
        "bucket": s3_input_bucket,
        "fixture_data": fixture_files,
        "key": message_key,
        "single_topic": "Y",
        "skip_encryption": skip_encryption,
        "kafka_message_volume": kafka_message_volume,
        "kafka_random_key": kafka_random_key,
        "topic_prefix": topic_prefix,
    }
    publish_message_to_sns(message, arn_value)

    if wait_for_job_completion:
        wait_for_dynamodb_kafka_job_to_finish(dynamodb_table_name, topic_name)


def wait_for_dynamodb_kafka_job_to_finish(dynamodb_table_name, topic_name):
    """Checks Dynamodb Kafka job to work out once it has finished.

    Keyword arguments:
    dynamodb_table_name -- the name of the dynamodb table to use
    topic_name -- the name for the topic to add to the prefix
    """

    retry = 0
    timeout_period = 900  # in seconds

    console_printer.print_info(
        "Waiting for file to be sent to the Kafka broker by Kafka producer lambda"
    )

    short_topic_name = template_helper.get_short_topic_name(topic_name)
    key_dict = {"JobId": {"S": f"{short_topic_name}"}}

    status = get_item_from_dynamodb(dynamodb_table_name, key_dict)
    while retry < timeout_period:
        if "Item" not in status:
            time.sleep(1)
            status = get_item_from_dynamodb(dynamodb_table_name, key_dict)
            retry += 1
        elif status["Item"]["JobStatus"]["S"] in ("QUEUED", "RUNNING"):
            time.sleep(1)
            status = get_item_from_dynamodb(dynamodb_table_name, key_dict)
            retry += 1
        elif status["Item"]["JobStatus"]["S"] in ("FAILED"):
            raise Exception(
                "Kafka producer lamdba encountered an error when sending messages to Kafka, check Kafka producer logs"
            )
        else:
            break

    if retry == timeout_period:
        raise Exception(
            "Kafka producer lamdba timed out when sending messages to Kafka, check Kafka producer logs"
        )

    assert status["Item"]["JobStatus"]["S"] == "SUCCESS"


def publish_message_to_sns(message, sns_topic_arn):
    """Post an SNS message to the topic that then sends it to the Kafka broker.

    Keyword arguments:
    message -- the message to send to a json dict
    sns_topic_arn -- the arn on the topic to send to
    """
    sns_client = get_client(service_name="sns")
    json_message = json.dumps(message)

    sns_response = sns_client.publish(
        TopicArn=sns_topic_arn,
        Message=json_message,
    )

    return sns_response["MessageId"]


def execute_athena_query(output_location, query):
    """Executes the given individual query against athena and return the result.

    Keyword arguments:
    output_location -- the s3 location to output the results of the execution to
    query -- the query to execute
    """
    console_printer.print_info(
        f"Executing query and sending output results to '{output_location}'"
    )

    athena_client = get_client(service_name="athena")
    query_start_resp = athena_client.start_query_execution(
        QueryString=query, ResultConfiguration={"OutputLocation": output_location}
    )
    execution_state = poll_athena_query_status(query_start_resp["QueryExecutionId"])

    if execution_state != "SUCCEEDED":
        raise KeyError(
            f"Athena query execution failed with final execution status of '{execution_state}'"
        )

    return athena_client.get_query_results(
        QueryExecutionId=query_start_resp["QueryExecutionId"]
    )


def poll_athena_query_status(id):
    """Polls athena for the status of a query.

    Keyword arguments:
    id -- the id of the query in athena
    """
    athena_client = get_client(service_name="athena")
    time_taken = 1
    while True:
        query_execution_resp = athena_client.get_query_execution(QueryExecutionId=id)

        state = query_execution_resp["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            console_printer.print_info(
                f"Athena query execution finished in {str(time_taken)} seconds with status of '{state}'"
            )
            return state

        time.sleep(1)
        time_taken += 1


def execute_manifest_glue_job(
    job_name,
    cut_off_time_start,
    cut_off_time_end,
    margin_of_error,
    snapshot_type,
    import_type,
    import_prefix,
    export_prefix,
):
    """Executes the given job in aws glue.

    Keyword arguments:
    job_name -- the name of the job to execute
    cut_off_time_start -- the time to not report any timestamps before (use None for all)
    cut_off_time_end -- the time to not report any timestamps after
    margin_of_error -- the margin of error time for the job
    snapshot_type -- the type of snapshots the manifest were generated from (full or incremental)
    import_type -- the type of import manifests with no space, i.e. "streaming_main" or "streaming_equality"
    import_prefix -- the base s3 prefix for the import data
    export_prefix -- the base s3 prefix for the export data
    """
    console_printer.print_info(f"Executing glue job with name of '{job_name}'")

    import_prefix = import_prefix.rstrip("/")
    export_prefix = export_prefix.rstrip("/")

    cut_off_time_start_qualified = (
        "0" if cut_off_time_start is None else str(cut_off_time_start)
    )

    console_printer.print_info(
        f"Start time for job is '{cut_off_time_start_qualified}', import type is {import_type}, "
        + f"snapshot type is {snapshot_type}, end time is '{cut_off_time_end}', "
        + f"import prefix of '{import_prefix}', export prefix of '{export_prefix}' "
        + f"and margin of error is '{margin_of_error}'"
    )

    glue_client = get_client(service_name="glue")
    job_run_start_result = glue_client.start_job_run(
        JobName=job_name,
        Arguments={
            "--cut_off_time_start": cut_off_time_start_qualified,
            "--cut_off_time_end": str(cut_off_time_end),
            "--margin_of_error": str(margin_of_error),
            "--import_type": import_type,
            "--snapshot_type": snapshot_type,
            "--import_prefix": import_prefix,
            "--export_prefix": export_prefix,
            "--enable-metrics": "",
        },
    )
    job_run_id = job_run_start_result["JobRunId"]
    console_printer.print_info(
        f"Glue job with name of {job_name} started run with id of {job_run_id}"
    )

    execution_state = poll_glue_job_run_status(job_name, job_run_id)
    if execution_state != "SUCCEEDED":
        raise KeyError(f"Glue job run failed with final status of '{execution_state}'")

    return True


def poll_glue_job_run_status(job_name, job_run_id):
    """Polls aws glue for the status of a job run.

    Keyword arguments:
    job_name -- the name of the job for this job run
    job_run_id -- the id of the specific job run
    """
    glue_client = get_client(service_name="glue")
    time_taken = 1
    while True:
        if time_taken % 1800 == 0:
            clear_session()
            glue_client = get_client(service_name="glue")

        job_run_result = glue_client.get_job_run(
            JobName=job_name, RunId=job_run_id, PredecessorsIncluded=False
        )

        job_run_state = job_run_result["JobRun"]["JobRunState"]

        if job_run_state in ("FAILED", "STOPPED", "SUCCEEDED"):
            console_printer.print_info(
                f"Glue job run finished in {str(time_taken)} seconds with status of "
                + f"'{job_run_state}'"
            )

            if job_run_state != "SUCCEEDED":
                job_run_error_message = job_run_result["JobRun"]["ErrorMessage"]
                console_printer.print_error_text(
                    f"Glue job run error message '{job_run_error_message}"
                )

            return job_run_state

        time.sleep(1)
        time_taken += 1


def retrieve_files_from_s3_with_bucket_and_path(s3_client, bucket, prefix):
    """Retrieves files from S3 with the given s3 client

    Keyword arguments:
    s3_client -- S3 client
    bucket -- S3 bucket
    prefix -- S3 prefix
    """
    s3_client_qualified = (
        s3_client if s3_client is not None else get_client(service_name="s3")
    )
    return s3_client_qualified.list_objects(Bucket=bucket, Prefix=prefix)["Contents"]


def get_s3_object(s3_client, bucket, key):
    """Retrieves objects from S3 with the given s3 client

    Keyword arguments:
    s3_client -- S3 client
    bucket -- S3 bucket
    key -- S3 key
    """
    s3_client_qualified = (
        s3_client if s3_client is not None else get_client(service_name="s3")
    )
    s3_object = s3_client_qualified.get_object(Bucket=bucket, Key=key)
    return s3_object["Body"].read()


def get_s3_object_metadata(bucket, key, s3_client=None):
    """Retrieves objects from S3 with the given s3 client

    Keyword arguments:
    s3_client -- S3 client
    bucket -- S3 bucket
    key -- S3 key
    """
    s3_client_qualified = (
        s3_client if s3_client is not None else get_client(service_name="s3")
    )
    response = s3_client_qualified.head_object(Bucket=bucket, Key=key)
    return response["Metadata"]


def get_asg_desired_count(autoscaling_client, asg_prefix):
    """Retrieves the desired count from the given asg

    Keyword arguments:
    autoscaling_client -- Autoscaling boto3 client (if None, will create one)
    asg_prefix -- the name of the ASG
    """
    matching_asg = []
    desired_count = None
    autoscaling_client_qualified = (
        autoscaling_client
        if autoscaling_client is not None
        else get_client(service_name="autoscaling")
    )

    try:
        response = autoscaling_client_qualified.describe_auto_scaling_groups()
    except ClientError as error:
        console_printer.print_info(
            "Could not get response from describing ASGs as the following error occurred: '{}'".format(
                error
            )
        )
        return None

    all_asg = response["AutoScalingGroups"]
    for asg_count in range(len(all_asg)):
        if re.match("^" + asg_prefix, all_asg[asg_count]["AutoScalingGroupName"]):
            matching_asg.append(all_asg[asg_count]["AutoScalingGroupName"])
            desired_count = all_asg[asg_count]["DesiredCapacity"]
    if len(matching_asg) > 1:
        raise AssertionError(
            f"Multiple ASGs returned: '{matching_asg}' for prefix '{asg_prefix}'"
        )

    if desired_count is None:
        console_printer.print_info(
            f"Could not retrieve current desired count of ASG with prefix '{asg_prefix}' so will assume it is scaled down"
        )
        return 0

    return int(desired_count)


def scale_asg_if_desired_count_is_not_already_set(asg_prefix, desired_count):
    """Scales the given ASG if it needs to be scaled

    Keyword arguments:
    asg_prefix -- the prefix of the ASG
    desired_count -- the desired count of the asg as an int
    """
    current_asg_count = get_asg_desired_count(None, asg_prefix)

    if current_asg_count is None:
        console_printer.print_warning_text(
            f"Not scaling ASG as current count not returned for '{asg_prefix}'"
        )
    elif desired_count == 0 and current_asg_count == 0:
        console_printer.print_info(
            f"Not scaling ASG as current count '{current_asg_count}' and "
            f"desired count '{desired_count}' are equal for '{asg_prefix}'"
        )
    elif desired_count > 0 and (current_asg_count >= desired_count):
        console_printer.print_info(
            f"Not scaling ASG as current count '{current_asg_count}' is equal or above "
            f"desired count '{desired_count}' for '{asg_prefix}')"
        )
    else:
        console_printer.print_info(
            f"Scaling ASG as current count '{current_asg_count}' is below "
            f"desired count '{desired_count}' for '{asg_prefix}'"
        )
        resize_auto_scaling_group(asg_prefix, desired_count)

    return (asg_prefix, desired_count)


def invoke_rbac_lambda(database_name, table, proxy_user):
    """Invokes the rbac lambda to test user permissions

    Keyword arguments:
    database_name -- the database to access
    pii_table_name -- the table that holds the (fake) pii data
    non_pii_table_name -- the table that holds non_pii data
    user_name -- which user to run the query as
    """
    payload_dict = {"db_name": database_name, "table": table, "proxy_user": proxy_user}
    payload_json = json.dumps(payload_dict)
    return invoke_lambda.invoke_rbac_test(payload_json)


def add_step_to_emr_cluster(step_flow, cluster_id):
    """Adds the given step to run on the given EMR cluster and returns the id of the step

    Keyword arguments:
    step_flow -- the step flow json, see https://docs.aws.amazon.com/emr/latest/ManagementGuide/emrfs-cli-reference.html#emrfs-submit-steps-as-cli
    cluster_id -- the id of the cluster to add the step to
    """
    client = get_client(service_name="emr")
    try:
        response = client.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step_flow])
    except Exception as e:
        console_printer.print_error_text(e.response["Error"]["Message"])
        raise AssertionError(e)

    return response["StepIds"][0]


def poll_emr_cluster_step_status(step_id, cluster_id, timeout_in_seconds=None):
    """Polls athena for the status of a query.

    Keyword arguments:
    step_id -- the id of the job step
    cluster_id -- the id of the cluster to add the step to
    timeout_in_seconds -- seconds to timeout as an int (or None for no timeout)
    """
    client = get_client(service_name="emr")

    time_taken = 1
    timeout_time = (
        None if timeout_in_seconds is None else time.time() + timeout_in_seconds
    )

    while timeout_time is None or time.time() < timeout_time:
        response = client.describe_step(ClusterId=cluster_id, StepId=step_id)

        state = response["Step"]["Status"]["State"]
        if state in ("COMPLETED", "CANCELLED", "FAILED", "INTERRUPTED"):
            console_printer.print_info(
                f"EMR job step with step id of {step_id} finished in {str(time_taken)} seconds with status of '{state}'"
            )
            return state

        time.sleep(1)
        time_taken += 1

        if time_taken % 600 == 0:
            clear_session()
            client = get_client(service_name="emr")

    raise AssertionError(
        f"EMR job step with step id of {step_id} did not finish within '{timeout_in_seconds}' seconds"
    )


def get_emr_cluster_step(step_name, cluster_id):
    """Returns the most recent step for a given step name if found in the cluster

    Keyword arguments:
    step_id -- the id of the job step
    cluster_id -- the id of the cluster to add the step to
    """
    client = get_client(service_name="emr")
    response = client.list_steps(ClusterId=cluster_id)
    for step in response["Steps"]:
        if step_name == step["Name"]:
            return step


def get_newest_emr_cluster_id(cluster_name, cluster_statuses=None):
    """Gets the newest emr cluster with the given name

    Keyword arguments:
    cluster_name -- the name of the cluster to find
    cluster_statuses -- (aaray) if provided, only return the cluster if the status matches one of these
    """
    client = get_client(service_name="emr")

    marker = None
    clusters = []

    while True:
        if marker and cluster_statuses:
            response = client.list_clusters(
                ClusterStates=cluster_statuses, Marker=marker
            )
        elif cluster_statuses:
            response = client.list_clusters(ClusterStates=cluster_statuses)
        elif marker:
            response = client.list_clusters(Marker=marker)
        else:
            response = client.list_clusters()

        clusters.extend(
            [
                cluster
                for cluster in response["Clusters"]
                if cluster["Name"] == cluster_name
            ]
        )

        if "Marker" in response:
            marker = response["Marker"]
        else:
            break

    latest_cluster = None
    for cluster in clusters:
        if (
            latest_cluster is None
            or cluster["Status"]["Timeline"]["CreationDateTime"]
            > latest_cluster["Status"]["Timeline"]["CreationDateTime"]
        ):
            latest_cluster = cluster

    return latest_cluster["Id"] if latest_cluster else None


def terminate_emr_cluster(cluster_id):
    """Terminates the given EMR cluster

    Keyword arguments:
    cluster_id -- the id of the cluster to terminate
    """
    console_printer.print_info(f"Terminating EMR cluster with id of '{cluster_id}'")

    client = get_client(service_name="emr")
    client.terminate_job_flows(JobFlowIds=[cluster_id])


def invoke_lambda_function(function_name, payload=None):
    """Invoke the given lambda and return the decoded payload response.

    Keyword arguments:
    function_name -- Name of the lambda function
    payload -- payload for triggering the lambda (can be None)
    """
    client = get_client(service_name="lambda", read_timeout_seconds=900)

    if payload is None:
        response = client.invoke(
            FunctionName=function_name,
            InvocationType="RequestResponse",
            LogType="None",
        )
    else:
        response = client.invoke(
            FunctionName=function_name,
            InvocationType="RequestResponse",
            LogType="None",
            Payload=payload,
        )
    status_code = response["StatusCode"]
    if not status_code == 200:
        raise AssertionError(
            f"Error in triggering the process, expected 200 but was {status_code}"
        )

    return response["Payload"].read()


def get_filtered_ecs_service_names(ecs_client, ecs_cluster_name, name_prefix):
    """Retrives the service names for the given cluster, using an optional regex

    Keyword arguments:
    ecs_client -- Autoscaling boto3 client (if None, will create one)
    ecs_cluster_name -- the name of the cluster the service is in
    name_prefix -- the prefix to filter the names
    """
    service_names = []
    ecs_client_qualified = (
        ecs_client if ecs_client is not None else get_client(service_name="ecs")
    )

    service_arns = ecs_client_qualified.list_services(cluster=ecs_cluster_name)[
        "serviceArns"
    ]

    batch_size = 10
    for i in range(0, len(service_arns), batch_size):
        batch = service_arns[i : i + batch_size]
        services = ecs_client_qualified.describe_services(
            cluster=ecs_cluster_name, services=batch
        )
        service_names_for_batch = [
            service["serviceName"] for service in services["services"]
        ]
        for service_name in service_names_for_batch:
            if service_name.startswith(name_prefix):
                service_names.append(service_name)

    console_printer.print_info(f"Retrieved service name list as '{service_names}'")

    return service_names


def get_ecs_service_desired_task_count(ecs_client, ecs_cluster_name, ecs_service_name):
    """Retrieves the desired count from the given ECS service

    Keyword arguments:
    ecs_client -- Autoscaling boto3 client (if None, will create one)
    ecs_cluster_name -- the name of the cluster the service is in
    ecs_service_name -- the name of the service
    """
    matching_asg = []
    desired_task_count = None
    ecs_client_qualified = (
        ecs_client if ecs_client is not None else get_client(service_name="ecs")
    )

    try:
        response = ecs_client_qualified.describe_services(
            cluster=ecs_cluster_name, services=[ecs_service_name]
        )
    except ClientError as error:
        console_printer.print_info(
            "Could not get response from describing ECS service as the following error occurred: '{}'".format(
                error
            )
        )
        return None

    return int(response["services"][0]["desiredCount"])


def scale_ecs_service_if_desired_count_is_not_already_set(
    ecs_cluster_name, ecs_service_name, desired_task_count
):
    """Scales the given ECS service if it needs to be scaled

    Keyword arguments:
    ecs_cluster_name -- the name of the cluster the service is in
    ecs_service_name -- the name of the service
    desired_task_count -- the desired task count of the ECS service as an int
    """
    for service_name in get_filtered_ecs_service_names(
        None, ecs_cluster_name, ecs_service_name
    ):
        current_task_count = get_ecs_service_desired_task_count(
            None, ecs_cluster_name, service_name
        )

        if current_task_count is None:
            console_printer.print_info(
                f"Not scaling ECS service named '{service_name}' as current count not returned"
            )
            return

        if desired_task_count == 0 and current_task_count == 0:
            console_printer.print_info(
                f"Not scaling ECS service named '{service_name}' as current and desired count are both 0"
            )
            return

        if desired_task_count > 0 and (current_task_count >= desired_task_count):
            console_printer.print_info(
                f"Not scaling ECS service named '{service_name}' as current count ({current_task_count}) if same as or above desired count ({desired_task_count})"
            )
            return

        console_printer.print_info(
            f"Scaling ECS service named '{service_name}' as current count ({current_task_count}) if below desired count ({desired_task_count})"
        )

        scale_ecs_service_desired_task_count(
            ecs_cluster_name, service_name, desired_task_count
        )


def scale_ecs_service_desired_task_count(
    ecs_cluster_name, ecs_service_name, desired_task_count
):
    """Scales the desired task count for the given ECS service

    Keyword arguments:
    ecs_cluster_name -- the name of the cluster the service is in
    ecs_service_name -- the name of the service
    desired_task_count -- the desired task count of the ECS service as an int
    """
    ecs_client = get_client(service_name="ecs")

    ecs_client.update_service(
        cluster=ecs_cluster_name,
        service=ecs_service_name,
        desiredCount=desired_task_count,
    )


def kms_decrypt_cipher_text(cipher_text_blob, aws_region=None):
    """Decrypts the given KMS cipher text and returns the plain text response

    Keyword arguments:
    cipher_text_blob -- the key's cipher text blob to decrypt
    ecs_service_name -- the name of the service
    desired_task_count -- the desired task count of the ECS service as an int
    aws_region -- if not the standard region, pass in here, else None (default)
    """
    kms_client = get_client(service_name="kms", region=aws_region)

    console_printer.print_info(f"Decrypting KMS cipher text of '{cipher_text_blob}'")

    kms_response = kms_client.decrypt(CiphertextBlob=cipher_text_blob)

    plain_text = kms_response["Plaintext"]

    console_printer.print_info(f"Plain text response returned as '{plain_text}'")

    return plain_text


def ssm_get_parameter_value(parameter_name, decrypt=False):
    """Returns the given paramater value from SSM

    Keyword arguments:
    parameter_name -- the name of the parameter to get
    decrypt -- True to decrypt the response (default is False)
    """
    ssm_client = get_client(service_name="ssm")

    console_printer.print_info(f"Retrieving SSM paramater called '{parameter_name}'")

    parameter = ssm_client.get_parameter(
        Name="/ucfs/claimant-api/nino/salt", WithDecryption=decrypt
    )

    return parameter["Parameter"]["Value"]


def attempt_assume_role(s3_client, max_tries):
    tries = 0
    while tries <= max_tries:
        try:
            time.sleep(10)
            s3_client = get_client("s3")
            break
        except ClientError as e:
            if e.response["Error"]["Code"] == "AccessDenied":
                tries += 1
                console_printer.print_warning_text(f"Try Number {tries}")
            else:
                raise e
    return s3_client


def test_s3_access_read(s3_bucket, key, s3_client=None):
    """Attempts to read a given file at the given s3 location

    Keyword arguments:
        s3_bucket -- The s3 bucket name containing the file
        key -- The name of the file to read including path
        s3_client -- The established s3 client to use
    """

    if s3_client is None:
        s3_client = attempt_assume_role(s3_client, 5)

    try:
        get_s3_object(s3_client, s3_bucket, key)
        return True
    except ClientError as e:
        if "GetObject operation: Access Denied" in str(e):
            return False
        else:
            raise e


def test_s3_access_write(s3_bucket, key, local_file, timeout, s3_client=None):
    """Attempts to read a given file at the given s3 location

    Keyword arguments:
        s3_bucket -- The s3 bucket name containing the file
        key -- The name of the file to read including path
        s3_client -- The established s3 client to use
    """

    if s3_client is None:
        s3_client = attempt_assume_role(s3_client, 5)

    try:
        upload_file_to_s3_and_wait_for_consistency(
            local_file, s3_bucket, timeout, key, s3_client
        )
        return True

    #   Raises ClientError whilst s3 call is happening asynchronously then returns S3UploadFailedError from the api call
    except ClientError as e:
        if "PutObject operation: Access Denied" in str(e):
            return False
        else:
            raise e
    except S3UploadFailedError as e:
        if "PutObject operation: Access Denied" in str(e):
            return False
        else:
            raise e


def create_role_and_wait_for_it_to_exist(
    role_name, assume_role_policy_document, iam_client=None
):
    """Attempts to create a role and waits for it to exist in AWS

    Keyword arguments:
       role_name -- The name of the role to create
       assume_role_policy_document -- The json for the assume role policy to use
       iam_client -- The established iam client to use
    """

    if iam_client is None:
        iam_client = get_client("iam")

    iam_client.create_role(
        RoleName=role_name, AssumeRolePolicyDocument=assume_role_policy_document
    )

    return wait_for_role_to_exist(role_name, iam_client)


def wait_for_role_to_exist(role_name, iam_client):
    """Waits for role to exist in AWS or throws error

    Keyword arguments:
        role_name -- The name of the role to create
        iam_client -- The established iam client to use
    """

    if iam_client is None:
        iam_client = get_client("iam")
    waiter = iam_client.get_waiter("role_exists")

    waiter.wait(RoleName=role_name, WaiterConfig={"Delay": 2, "MaxAttempts": 60})

    return role_name


def attach_policies_to_role(policy_arns, role_name, iam_client=None):
    """Attaches policies to a role

    Keyword arguments:
        policy_arns -- List of policy arns to attach to role
        role_name -- The name of the role to create
        iam_client -- The established iam client to use
    """

    if iam_client is None:
        iam_client = get_client("iam")

    for policy_arn in policy_arns:
        iam_client.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)

    wait_for_policy_to_be_attached_to_role(role_name)


def wait_for_policy_to_be_attached_to_role(role_name, iam_client=None):
    """Waits for policy to attach to a role

    Keyword arguments:
        role_name -- The name of the role the policy is being attached to
        iam_client -- The established iam client to use
    """

    if iam_client is None:
        iam_client = get_client("iam")

    policy_count = 0
    tries = 0

    while (policy_count == 0) and (tries < 5):
        try:
            policies = iam_client.list_attached_role_policies(RoleName=role_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchEntityException":
                pass
            else:
                raise e
        policy_count = len(policies["AttachedPolicies"])
        tries += 1
        time.sleep(5)


def remove_role(role_name, policy_arns, iam_client=None):
    """Deletes a role

    Keyword arguments:
        role_name -- The name of the role to delete from aws
        policy_arns -- List of policy arns attached to role
        iam_client -- The established iam client to use
    """

    if iam_client is None:
        iam_client = get_client("iam")

    for policy in policy_arns:
        iam_client.detach_role_policy(RoleName=role_name, PolicyArn=policy)

    iam_client.delete_role(RoleName=role_name)


def list_policy_arns_for_role(role_name, account_id, iam_client=None):
    """Takes a role name and returns a list of arns for all policies attached to it

    Keyword arguments:
        role_name -- The name of the role to delete from aws
        account_id -- AWS account id
        iam_client -- The established iam client to use
    """

    if iam_client is None:
        iam_client = get_client("iam")

    response = iam_client.list_attached_role_policies(
        RoleName=role_name,
    )

    return [policy.get("PolicyArn") for policy in response.get("AttachedPolicies")]


def list_steps(cluster_id):
    """Takes cluster id and lists steps names
    Keyword arguments:
        cluster_id -- The id of the given emr cluster
    """
    client = get_client(service_name="emr")
    try:
        response = client.list_steps(ClusterId=cluster_id)
    except Exception as e:
        console_printer.print_error_text(e.response["Error"]["Message"])
        raise AssertionError(e)

    return [response["Steps"][i]["Id"] for i in range(len(response["Steps"]))]


def generate_arn(service, arn_suffix, region=None):
    """Returns a formatted arn for AWS.

    Keyword arguments:
        service -- the AWS service
        arn_suffix -- the majority of the arn after the initial common data
        region -- the region (can be None for region free arns)
    """
    arn_value = "arn"
    aws_value = "aws"
    region_qualified = region if region else ""

    return f"{arn_value}:{aws_value}:{service}:{region_qualified}:{arn_suffix}"


def check_tags_of_cluster(cluster_id, emr_client=None):
    """Adds additional tags to an EMR cluster and its instances.

    Keyword arguments:
    cluster_id -- the id of the cluster
    dict_of_tags -- a dictionary of key value pairs. eg. {"Correlation_Id": "test"}
    emr_client -- client to override the standard one
    """
    if emr_client is None:
        emr_client = get_client(service_name="emr")

    response = emr_client.describe_cluster(ClusterId=cluster_id)
    cluster_tags = response["Cluster"]["Tags"]

    return cluster_tags


def check_if_s3_object_exists(bucket, key, s3_client=None):
    """Returns True or False based on object existing in s3 location

    Keyword arguments:
       bucket -- the s3 bucket id
       key -- the key/prefix for the location of the file
       s3_client -- an established s3 client (optional)
    """

    if s3_client == None:
        s3_client = get_client(service_name="s3")

    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=key,
    )
    for obj in response.get("Contents", []):
        if obj["Key"] == key:
            return True

    return False


def execute_commands_on_ec2_by_tags_and_wait(
    commands: list, ec2_tags: list, timeout: int, ssm_client=None
):
    """Executes command on ec2 instances filtered by tags

    Keyword arguments:
       commands -- Array of string commands to run
       key -- Array of Application names the instances are tagged with
       timeout -- The time in seconds to wait for the commands to be run
       ssm_client -- an established ssm client (optional)
    """

    if not ssm_client:
        ssm_client = get_client("ssm")

    resp = ssm_client.send_command(
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": commands},
        TimeoutSeconds=30,
        Targets=[{"Key": "tag:Application", "Values": ec2_tags}],
    )

    console_printer.print_info(f"Response from ssm {resp}")
    time.sleep(timeout)


def purge_sqs_queue(queue_name, aws_region="eu-west-2"):
    console_printer.print_info(f"Purging queue: {queue_name}")
    service_name = "sqs"
    client = get_client(service_name=service_name, region=aws_region)
    queue_url = client.get_queue_url(QueueName=queue_name)["QueueUrl"]
    client.purge_queue(QueueUrl=queue_url)


def execute_linux_command(
    instance_id, linux_command, username="root", aws_region="eu-west-2"
):
    service_name = "ssm"
    cmd = f"sudo su -c '{linux_command}' -s /bin/sh {username}"

    client = get_client(service_name=service_name, region=aws_region)
    response = client.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": [cmd]},
    )
    time.sleep(10)

    command_id = response["Command"]["CommandId"]
    output = client.get_command_invocation(InstanceId=instance_id, CommandId=command_id)

    return output


def get_ssm_parameter_value(ssm_parameter_value, aws_region="eu-west-2"):
    service_name = "ssm"
    client = get_client(service_name=service_name, region=aws_region)

    return client.get_parameter(Name=ssm_parameter_value, WithDecryption=True)[
        "Parameter"
    ]["Value"]


def trigger_batch_job(
    job_name: str,
    job_queue_name: str,
    job_definition: str,
    parameters=None,
) -> str:
    """
    Triggers a batch job given job_name, job_queue_name, job_definition, and optionally
    additional parameters.  Returns jobId of submitted job
    """
    client = get_client("batch")
    response = client.submit_job(
        jobName=job_name,
        jobQueue=job_queue_name,
        jobDefinition=job_definition,
        parameters=parameters,
    )
    return response["jobId"]


def poll_batch_queue_for_job(
    job_queue_name: str,
    timeout_in_seconds=None,
):

    statuses = [
        "SUBMITTED",
        "PENDING",
        "RUNNABLE",
        "STARTING",
        "RUNNING",
        "SUCCEEDED",
        "FAILED",
    ]
    client = get_client("batch")
    timeout_time = None if not timeout_in_seconds else time.time() + timeout_in_seconds
    while timeout_time is None or timeout_time > time.time():
        responses = [
            client.list_jobs(
                jobQueue=job_queue_name,
                jobStatus=status,
            )
            for status in statuses
        ]
        active_job_list = [
            job["jobId"]
            for response in responses
            for job in response["jobSummaryList"]
            if job["status"] not in ["FAILED", "SUCCEEDED"]
        ]

        if len(active_job_list) > 0:
            return active_job_list
        else:
            console_printer.print_info("Waiting for batch job to be submitted")
            time.sleep(5)
            continue
    raise AssertionError("Timed out waiting for batch job to be submitted")


def poll_batch_job_status(
    job_id,
    timeout_in_seconds=None,
):
    client = get_client("batch")
    timeout_time = None if not timeout_in_seconds else time.time() + timeout_in_seconds

    while timeout_time is None or timeout_time > time.time():
        response = client.describe_jobs(jobs=[job_id])
        status = response["jobs"][0]["status"]
        console_printer.print_info(f"Job status: {status}")
        if status in ["FAILED", "SUCCEEDED"]:
            return status
        else:
            time.sleep(5)

    raise AssertionError(f"Timed out waiting for batch job in queue")


def get_instance_id(instance_name, region_name="eu-west-2"):
    service_name = "ec2"
    client = get_client(service_name=service_name, region=region_name)
    filters = [
        {"Name": "tag:Name", "Values": [instance_name]},
        {"Name": "instance-state-name", "Values": ["running"]},
    ]

    response = client.describe_instances(Filters=filters)
    instance_id = response["Reservations"][0]["Instances"][0]["InstanceId"]
    return instance_id
