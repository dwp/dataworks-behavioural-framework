import boto3
import os
import requests
import datetime
import json
import base64
import hashlib
import yaml
from aws_requests_auth.aws_auth import AWSRequestsAuth
from helpers import (
    aws_helper,
    console_printer
)


def query_for_claimant_from_claimant_api(
        aws_host_name,
        claimant_api_region,
        award_details_api_path, 
        hashed_nino, 
        transaction_id=None, 
        from_date=None, 
        to_date=None
    ):
    """Querys for specific claimant and returns the response

    Keyword arguments:
    aws_host_name -- the host name of the claimant API to request from
    claimant_api_region -- the region to use for this claimant API test
    award_details_api_path -- the API path of the award details function
    hashed_nino -- the national insurance number already hashed
    transaction_id -- the id of the transaction (optional)
    from_date -- the date to look from in format YYYYMMDD (optional)
    to_date -- the date to look to in format YYYYMMDD (optional)
    """
    console_printer.print_info(
        f"Querying for claimant with nino of '{hashed_nino}', transactionId of '{transaction_id}', fromDate of '{from_date}' and toDate of '{to_date}'"
    )

    request_body = {
        "nino": hashed_nino,
    }

    if transaction_id is not None:
        request_body["transactionId"] = transaction_id

    if from_date is not None:
        request_body["fromDate"] = from_date

    if to_date is not None:
        request_body["toDate"] = to_date

    console_printer.print_info(f"Request body built as '{request_body}'")

    return send_request_to_claimant_api(aws_host_name, award_details_api_path, request_body, claimant_api_region)


def send_request_to_claimant_api(aws_host_name, api_path, request_body, claimant_api_region):
    """Sends the given request to the Claimant API

    Keyword arguments:
    aws_host_name -- the host name of the claimant API to request from
    api_path -- the API path to request from
    request_body -- the request body as json
    claimant_api_region -- the region to use for this claimant API test
    """
    if api_path.startswith("/"):
        api_path = api_path[1:]

    full_path = os.path.join("ucfs-claimant", api_path)

    default_credentials = aws_helper.get_session_credentials().get_frozen_credentials()

    auth = AWSRequestsAuth(aws_access_key=default_credentials.access_key,
                        aws_secret_access_key=default_credentials.secret_key,
                        aws_token=default_credentials.token,
                        aws_host=aws_host_name,
                        aws_region=claimant_api_region,
                        aws_service="execute-api")

    request_url = f'https://{aws_host_name}/{full_path}'
    headers = {'Content-Type':'application/json',
            'X-Amz-Date':datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}

    console_printer.print_info(
        f"Sending request to '{request_url}' with auth of '{auth}' and headers of '{headers}'"
    )

    response = requests.post(request_url, data=json.dumps(request_body), auth=auth, headers=headers)
    return (response.status_code, response.json())


def get_topic_by_id_type(id_field_name):
    """Returns the topic name for the corresponding id type

    Keyword arguments:
    id_field_name -- the id field name for the kafka files
    """
    if id_field_name.lower() == "citizenid":
        return "db.core.claimant"
    elif id_field_name.lower() == "contractid":
        return "db.core.contract"
    elif id_field_name.lower() == "statementid":
        return "db.core.statement"
    
    raise AssertionError(f"Unsupported claimant id field type of '{id_field_name}'")


def hash_nino(nino, salt):
    """Hashes the given nino for claimant API requests

    Keyword arguments:
    nino -- the nino to hash
    salt -- the salt used to hash
    """
    sha = hashlib.sha512()
    sha.update(nino.encode())
    sha.update(salt.encode())
    digest_b64 = base64.b64encode(sha.digest()).decode().replace('+', '-').replace('/', '_')
    return digest_b64


def retrieve_assessment_periods_from_claimant_data_file(input_data_file_name, fixture_files_root, fixture_data_folder):
    """Gets all the assessment periods from the given data file and returns them as an array of arrays

    Keyword arguments:
    input_data_file_name -- the input file name containing the data
    fixture_files_root -- the local path to the feature file to send
    fixture_data_folder -- the folder from the root of the fixture data
    """
    data_file_name = os.path.join(fixture_files_root, fixture_data_folder, input_data_file_name)

    console_printer.print_info(
        f"Retrieving assessment periods from data file at '{data_file_name}'"
    )

    return_data = []
    input_data = yaml.safe_load(open(data_file_name))
    for item in input_data:
        if "assessment_periods" in item:
            for assessment_period in item["assessment_periods"]:
                return_data.append(assessment_period)

    console_printer.print_info(
        f"Successfully retrieved '{len(return_data)}' assessment periods"
    )
    
    return return_data
