import json
import os
import shutil
from datetime import datetime
from helpers import console_printer, date_helper

_base_datetime_created = datetime.strptime(
    "2017-10-03T05:04:03.003", "%Y-%m-%dT%H:%M:%S.%f"
)


def get_id_object_from_json_file(fixture_file_local):
    """Returns the _id object for the given valid json file.

    Keyword arguments:
    fixture_file_local -- the file to get the id from (must be valid json input)
    """
    with open(fixture_file_local) as open_file:
        input_data = json.load(open_file)

    return get_id_object_from_json(input_data)


def get_id_object_from_json(json_data):
    """Returns the _id object for the given valid json.

    Keyword arguments:
    json_data -- the valid json data input
    """
    return json_data["key"] if "key" in json_data else json_data["message"]["_id"]


def get_timestamp_as_long_from_json_file(fixture_file_local):
    """Returns the timestamp object for the given valid json file as a long.

    Keyword arguments:
    fixture_file_local -- the file to get the id from (must be valid json input)
    """
    with open(fixture_file_local) as open_file:
        json_data = json.load(open_file)

    timestamp_created_from = json_data["message"]["timestamp_created_from"]
    timestamp_as_date = (
        "1980-01-01T00:00:00.000+0000"
        if timestamp_created_from == "epoch"
        else json_data["message"]["dbObject"][timestamp_created_from]["$date"]
    )
    formatted_timestamp = date_helper.format_time_to_timezome_free(timestamp_as_date)

    return date_helper.generate_milliseconds_epoch_from_timestamp(
        formatted_timestamp[:-1]
    )


def get_first_id_from_json_file(fixture_file_local):
    """Returns the first _id value for the given valid json file.
    Keyword arguments:
    fixture_file_local -- the file to get the id from (must be valid json input)
    """
    with open(fixture_file_local) as open_file:
        input_data = json.load(open_file)
        return get_first_id_from_json(input_data)


def get_first_id_from_json(json_data):
    """Returns the first _id value for the given valid json.
    Keyword arguments:
    json_data -- the valid json data input
    """
    id_object = get_id_object_from_json(json_data)
    id_field = list(id_object.keys())[0]
    return id_object[id_field]


def generate_edited_files_folder(local_files_temp_folder, input_folder):
    """Creates and returns the location of an temporary edited files folder to use.

    Keyword arguments:
    local_files_temp_folder -- the root folder for the temporary files to sit in
    input_folder -- the parent folder for the files to sit it
    """
    directory = os.path.join(
        local_files_temp_folder,
        input_folder,
        "edited_files",
    )

    if not os.path.exists(directory):
        os.makedirs(directory)

    return directory


def generate_edited_file_name(folder_name, file_name):
    """Generates a standard name for an edited local file.

    Keyword arguments:
    folder_name -- the folder for the edited files
    file_name -- the name of the file that has been edited
    """
    return os.path.join(
        folder_name,
        "edited-" + file_name,
    )


def get_contents_of_files_in_folder(folder, remove_whitespace):
    """Return a list of the content of files in a given folder.

    Keyword arguments:
    folder -- the folder to look in
    remove_whitespace -- remove new lines and spaces
    """
    file_contents = []
    files = os.listdir(folder)

    for file in files:
        file_contents.append(
            get_contents_of_file(os.path.join(folder, file), remove_whitespace)
        )

    return file_contents


def get_file_from_folder_with_latest_timestamp(folder):
    """Return the file which has the latest timestamp.

    Keyword arguments:
    folder -- the folder to look in
    """
    latest_file = ""
    date_pattern = "%Y-%m-%dT%H:%M:%S.%f%z"
    latest_timestamp = None

    files = os.listdir(folder)

    for file in files:
        full_path = os.path.join(folder, file)

        with open(full_path) as open_file:
            json_data = json.load(open_file)

        message = json.loads(json.dumps(json_data["message"]))

        if "_lastModifiedDateTime" in message:
            dateTimeString = message["_lastModifiedDateTime"]
        elif "createdDateTime" in message:
            dateTimeString = message["createdDateTime"]
        else:
            dateTimeString = "1980-01-01T00:00:00.000+0000"

        current_timestamp = datetime.strptime(dateTimeString, date_pattern)
        if latest_timestamp is None or current_timestamp > latest_timestamp:
            latest_timestamp = current_timestamp
            latest_file = full_path

    console_printer.print_info(
        f"Latest timestamp is {latest_timestamp} from file {latest_file}"
    )

    return latest_file


def get_id_from_claimant_by_id(folder, match_value, match_type, return_type):
    """Return the citizen id for the claimant using the nino.

    Keyword arguments:
    folder -- the folder to look in
    match_value -- the value to look for to match the file
    match_type -- nino, citizenId, contractId or statementId in order to provide the matching field
    return_type -- nino, citizenId, contractId or statementId for the return field
    """
    console_printer.print_info(
        f"Retreiving type of '{return_type}' for matching value of '{match_value}' and type of '{match_type}' in folder '{folder}'"
    )

    files = os.listdir(folder)

    match_types_under_dbobject = ["nino", "people", "personid"]

    root_object_match = "_id"
    if match_type.lower() in match_types_under_dbobject:
        root_object_match = "dbObject"

    root_object_return = "_id"
    if return_type.lower() in match_types_under_dbobject:
        root_object_return = "dbObject"

    for file_name in files:
        full_path = os.path.join(folder, file_name)
        with open(full_path) as open_file:
            json_data = json.load(open_file)
        message = json.loads(json.dumps(json_data["message"]))

        if root_object_match in message and match_type in message[root_object_match]:
            if match_value in message[root_object_match][match_type]:
                if root_object_return in message and return_type in message[root_object_return]:
                    return_value = message[root_object_return][return_type]
                    console_printer.print_info(
                        f"Found returnable value of '{return_value}' in file '{file_name}'"
                    )
                    return return_value

    console_printer.print_error_text(
        f"Could not find '{return_type}' for matching value of '{match_value}' and type of '{match_type}' in folder '{folder}'"
    )

    raise AssertionError(f"Could not find '{return_type}'")


def get_contents_of_file(path_and_file_name, remove_whitespace):
    """Return the given file contents, formatted for assertion.

    Keyword arguments:
    path_and_file_name -- full path to relevant file
    remove_whitespace -- remove new lines and spaces
    """
    with open(path_and_file_name) as f:
        if remove_whitespace:
            return f.read().replace("\n", "").replace(" ", "")
        else:
            return f.read()


def clear_and_delete_directory(directory_path):
    """Clears the given directory AND deletes it.

    Keyword arguments:
    directory_path -- the full path to the directory
    """
    shutil.rmtree(directory_path)


def get_json_with_replaced_values(json_value):
    """Replaces the values for ignorable fields with a static value

    Keyword arguments:
    json_value -- the item for which the value has to be replaced
    """
    data = json.loads(json_value)

    if "message" in data:
        if "encryption" in data["message"]:
            data["message"]["encryption"] = "ignored value"
        if "dbObject" in data["message"]:
            data["message"]["dbObject"] = "ignored value"

    if "unitOfWorkId" in data:
        data["unitOfWorkId"] = "ignored value"

    if "timestamp" in data:
        data["timestamp"] = "ignored value"

    if "traceId" in data:
        data["traceId"] = "ignored value"

    if "put_time" in data:
        data["put_time"] = "ignored value"

    if "version" in data:
        data["version"] = "ignored value"

    if "body" in data:
        data["body"] = "ignored value"

    if "reason" in data:
        data["reason"] = "ignored value"

    data_formatted = json.dumps(data, sort_keys=True)
    return data_formatted


def format_json(json_value):
    """Formats the given json in a standard way.

    Keyword arguments:
    json_value -- the json to be formatted as a string
    """
    data = json.loads(json_value)
    data_formatted = json.dumps(data, sort_keys=True)
    return data_formatted


def generate_local_output_file(parent_folder, file_name, local_files_temp_folder):
    """Formats a local file name for outputting to.

    Keyword arguments:
    parent_folder -- the parent folder for the file
    file_name -- the file name to send
    local_files_temp_folder -- the root folder for the temporary files to sit in
    """
    edited_files_folder = generate_edited_files_folder(
        local_files_temp_folder, parent_folder
    )
    return generate_edited_file_name(edited_files_folder, file_name)


def create_local_file(file_name, path="./", file_contents=""):
    """Creates a local file to be used by tests

    file_name -- The name of the local file to be created
    path -- The path in which to create the local file with trailing slash - defaults to current dir
    file_contents -- The contents for the file - defaults to empty file
    """
    with open(path+file_name, "w") as file:
        file.write(file_contents)


def delete_local_file(file_name, path="./"):
    """Deletes a local file to be used by tests

    file_name -- The name of the local file to be deleted
    path -- The path to the local file - defaults to current dir
    """
    os.remove(path+file_name)
