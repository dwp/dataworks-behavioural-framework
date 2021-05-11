import os
import uuid
import base64
import time
import json
from datetime import datetime, timedelta
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from behave import given, when, then
from helpers import (
    ucfs_claimant_api_helper,
    console_printer,
    invoke_lambda,
    claimant_api_data_generator,
    streaming_data_helper,
    aws_helper,
    file_helper,
)


message_type = "claimant_api"


@given(
    "I create a data file of '{data_file_name}' for a claimant with multiple assessment periods, with take home pay values of '{take_home_pays}'"
)
def step_impl(context, data_file_name, take_home_pays):

    folder = streaming_data_helper.generate_fixture_data_folder(message_type)
    fixture_data_path = os.path.join(context.fixture_path_local, folder)

    ap_thp = take_home_pays.split(",")
    ap_thp_clean = [amount.strip(" ") for amount in ap_thp]

    assessment_periods = []
    for index, amount in enumerate(ap_thp_clean):
        if index == 0:
            start_date = datetime.strftime(
                claimant_api_data_generator._month_delta(datetime.today(), -1), "%Y%m%d"
            )
            end_date = datetime.strftime(datetime.now() - timedelta(days=1), "%Y%m%d")
        else:
            start_date = datetime.strftime(
                claimant_api_data_generator._month_delta(
                    datetime.today(), (index + 1) * -1
                ),
                "%Y%m%d",
            )
            end_date = datetime.strftime(
                claimant_api_data_generator._month_delta(datetime.today(), index * -1),
                "%Y%m%d",
            )
        assessment_periods.append(
            {"start_date": start_date, "end_date": end_date, "amount": amount}
        )

    console_printer.print_info(
        f"Generating UCFS claimant API data file "
        + f"with assessment periods, '{assessment_periods}'"
    )

    template_yml = "- assessment_periods:"

    for assessment_period in assessment_periods:
        yml = f'''
    - start_date: "{assessment_period['start_date']}"
      end_date: "{assessment_period['end_date']}"
      amount: "{assessment_period['amount']}"'''
        template_yml = template_yml + yml

    file_helper.create_local_file(data_file_name, f"{fixture_data_path}/", template_yml)

@given("Create a data file of '{data_file_name}' for a claimant with a completed assessment period with a take home pay of '{take_home_pay}' and a suspension date of '{suspension_date_offset}' from end date")
def step_impl(context, data_file_name, take_home_pay, suspension_date_offset):
    context.execute_steps(
        f"given I create a data file of '{data_file_name}' for a claimant with multiple assessment periods, with take home pay values of '{take_home_pay}'"
    )
    folder = streaming_data_helper.generate_fixture_data_folder(message_type)
    fixture_data_path = os.path.join(context.fixture_path_local, folder)

    file_path = os.path.join(fixture_data_path, data_file_name)
    file_contents = file_helper.get_contents_of_file(file_path, False)
    print(file_contents)

    console_printer.print_info(
        f"Adding a suspension date for claimant."
        + f"Take home pay: '{take_home_pay}', suspension date: '{suspension_date}'"
    )

@given("The claimant API '{region_type}' region is set to '{region}'")
def step_impl(context, region_type, region):
    region_to_use = region.lower()
    if region.lower() == "ireland":
        region_to_use = context.aws_region_alternative
    elif region.lower() == "london":
        region_to_use = context.aws_region_main

    if region_type.lower() == "storage":
        context.claimant_api_storage_region = region_to_use.lower()
    else:
        context.claimant_api_business_region = region_to_use.lower()

    console_printer.print_info(
        f"Claimant API region of type '{region_type}' set to '{region}'"
    )


@given("The nino salt has been retrieved")
def step_impl(context):
    context.nino_salt = aws_helper.ssm_get_parameter_value(
        context.ucfs_claimant_api_salt_ssm_parameter_name, True
    )

    console_printer.print_info(f"Claimant API nino salt set to '{context.nino_salt}'")


@given("The claimant API data has been reset")
def step_impl(context):
    invoke_lambda.invoke_claimant_mysql_metadata_interface()

    console_printer.print_info(f"Claimant API data reset")


@then("Take home pay can be successfully decoded as '{expected_value}'")
def step_impl(context, expected_value):
    assert (
        context.claimant_api_status_code == 200
    ), f"Status code from response was {context.claimant_api_status_code}, not 200"
    assert context.claimant_api_response is not None, f"Response body was empty"

    response = context.claimant_api_response

    try:
        key_id = response["assessmentPeriod"][0]["amount"]["keyId"]
        take_home_pay_enc = base64.urlsafe_b64decode(
            response["assessmentPeriod"][0]["amount"]["takeHomePay"]
        )
        cipher_text_blob = base64.urlsafe_b64decode(
            response["assessmentPeriod"][0]["amount"]["cipherTextBlob"]
        )
    except Exception as ex:
        console_printer.print_error_text(
            f"Could not retrieve information from claimant API response of '{response}' and error of '{ex}'"
        )
        raise ex

    console_printer.print_info(
        f"Successfully retrieved cipher text of '{cipher_text_blob}' and take home pay of '{take_home_pay_enc}'"
    )
    data_key = aws_helper.kms_decrypt_cipher_text(
        cipher_text_blob, context.claimant_api_storage_region
    )
    console_printer.print_info(f"Successfully decoded data key of '{data_key}'")

    nonce_size = 12
    nonce = take_home_pay_enc[:nonce_size]
    take_home_pay_data = take_home_pay_enc[nonce_size:]
    aesgcm = AESGCM(data_key)
    take_home_pay = aesgcm.decrypt(nonce, take_home_pay_data, None).decode("utf-8")

    console_printer.print_info(
        f"Successfully decoded take home pay of '{take_home_pay}'"
    )

    # This step is used by other scenarios which can have multiple values in the expected_value
    if "," not in expected_value:
        assert (
            take_home_pay == expected_value
        ), f"Take home pay was {take_home_pay} which does not match expected value of {expected_value}"
    else:
        expected_value = expected_value.split(",")
        assert (
            take_home_pay == expected_value[0]
        ), f"Take home pay was {take_home_pay} which does not match expected value of {expected_value[0]}"


@given(
    "UCFS send claimant API kafka messages with input file of '{input_file_name}' and data file of '{data_file_name}'"
)
def step_impl(
    context,
    input_file_name,
    data_file_name,
):
    global message_type

    root_folder = os.path.join(context.temp_folder, str(uuid.uuid4()))
    folder = streaming_data_helper.generate_fixture_data_folder(message_type)
    context.claimant_api_kafka_temp_folder = os.path.join(root_folder, folder)
    topic_prefix = streaming_data_helper.generate_topic_prefix(message_type)

    (
        kafka_input_file_data,
        context.generated_ninos,
        context.generated_ids,
    ) = claimant_api_data_generator.generate_claimant_api_kafka_files(
        s3_input_bucket=context.s3_ingest_bucket,
        input_data_file_name=data_file_name,
        input_template_name=input_file_name,
        new_uuid=None,
        local_files_temp_folder=root_folder,
        fixture_files_root=context.fixture_path_local,
        s3_output_prefix=context.s3_temp_output_path,
        seconds_timeout=context.timeout,
        fixture_data_folder=folder,
    )

    context.local_generated_claimant_api_kafka_files = []

    for (id_field_name, generated_files) in kafka_input_file_data:
        files_to_send = [db_object_tuple[0] for db_object_tuple in generated_files]
        context.local_generated_claimant_api_kafka_files.extend(
            [db_object_tuple[1] for db_object_tuple in generated_files]
        )
        aws_helper.send_files_to_kafka_producer_sns(
            dynamodb_table_name=context.dynamo_db_table_name,
            s3_input_bucket=context.s3_ingest_bucket,
            aws_acc_id=context.aws_acc,
            sns_topic_name=context.aws_sns_topic_name,
            fixture_files=files_to_send,
            message_key=uuid.uuid4(),
            topic_name=ucfs_claimant_api_helper.get_topic_by_id_type(id_field_name),
            topic_prefix=topic_prefix,
            region=context.aws_region_main,
            skip_encryption=False,
            kafka_message_volume="1",
            kafka_random_key="true",
            wait_for_job_completion=True,
        )


@when(
    "UCFS send kafka updates for first existing claimant with input file of '{input_file_name}' and data file of '{data_file_name}'"
)
def step_impl(
    context,
    input_file_name,
    data_file_name,
):
    global message_type

    existing_files_folder = os.path.join(
        context.claimant_api_kafka_temp_folder, "edited_files"
    )
    citizen_id = file_helper.get_id_from_claimant_by_id(
        existing_files_folder, context.generated_ninos[0], "nino", "citizenId"
    )
    contract_id = file_helper.get_id_from_claimant_by_id(
        existing_files_folder, citizen_id, "people", "contractId"
    )
    topic_prefix = streaming_data_helper.generate_topic_prefix(message_type)

    folder = streaming_data_helper.generate_fixture_data_folder(message_type)

    kafka_input_file_data = claimant_api_data_generator.generate_updated_contract_and_statement_files_for_existing_claimant(
        citizen_id=citizen_id,
        contract_id=contract_id,
        fixture_files_root=context.fixture_path_local,
        fixture_data_folder=folder,
        input_data_file_name=data_file_name,
        input_template_name=input_file_name,
        s3_input_bucket=context.s3_ingest_bucket,
        local_files_temp_folder=os.path.join(context.temp_folder, str(uuid.uuid4())),
        s3_output_prefix=context.s3_temp_output_path,
        seconds_timeout=context.timeout,
    )

    context.local_generated_claimant_api_kafka_files = []

    for (id_field_name, generated_files) in kafka_input_file_data:
        files_to_send = [db_object_tuple[0] for db_object_tuple in generated_files]
        context.local_generated_claimant_api_kafka_files.extend(
            [db_object_tuple[1] for db_object_tuple in generated_files]
        )
        aws_helper.send_files_to_kafka_producer_sns(
            dynamodb_table_name=context.dynamo_db_table_name,
            s3_input_bucket=context.s3_ingest_bucket,
            aws_acc_id=context.aws_acc,
            sns_topic_name=context.aws_sns_topic_name,
            fixture_files=files_to_send,
            message_key=uuid.uuid4(),
            topic_name=ucfs_claimant_api_helper.get_topic_by_id_type(id_field_name),
            topic_prefix=topic_prefix,
            region=context.aws_region_main,
            skip_encryption=False,
            kafka_message_volume="1",
            kafka_random_key="true",
            wait_for_job_completion=True,
        )


@when(
    "UCFS send a kafka delete for first existing claimant with input file of '{input_file_name}'"
)
def step_impl(
    context,
    input_file_name,
):
    global message_type

    existing_files_folder = os.path.join(
        context.claimant_api_kafka_temp_folder, "edited_files"
    )
    citizen_id = file_helper.get_id_from_claimant_by_id(
        existing_files_folder, context.generated_ninos[0], "nino", "citizenId"
    )
    person_id = file_helper.get_id_from_claimant_by_id(
        existing_files_folder, context.generated_ninos[0], "nino", "personId"
    )
    topic_prefix = streaming_data_helper.generate_topic_prefix(message_type)

    folder = streaming_data_helper.generate_fixture_data_folder(message_type)

    kafka_input_file_data = claimant_api_data_generator.generate_updated_claimant_file_for_existing_claimant(
        citizen_id=citizen_id,
        person_id=person_id,
        fixture_files_root=context.fixture_path_local,
        fixture_data_folder=folder,
        input_template_name=input_file_name,
        s3_input_bucket=context.s3_ingest_bucket,
        local_files_temp_folder=os.path.join(context.temp_folder, str(uuid.uuid4())),
        s3_output_prefix=context.s3_temp_output_path,
        seconds_timeout=context.timeout,
        increment=1,
    )

    context.local_generated_claimant_api_kafka_files = []

    for (id_field_name, generated_files) in kafka_input_file_data:
        files_to_send = [db_object_tuple[0] for db_object_tuple in generated_files]
        context.local_generated_claimant_api_kafka_files.extend(
            [db_object_tuple[1] for db_object_tuple in generated_files]
        )
        aws_helper.send_files_to_kafka_producer_sns(
            dynamodb_table_name=context.dynamo_db_table_name,
            s3_input_bucket=context.s3_ingest_bucket,
            aws_acc_id=context.aws_acc,
            sns_topic_name=context.aws_sns_topic_name,
            fixture_files=files_to_send,
            message_key=uuid.uuid4(),
            topic_name=ucfs_claimant_api_helper.get_topic_by_id_type(id_field_name),
            topic_prefix=topic_prefix,
            region=context.aws_region_main,
            skip_encryption=False,
            kafka_message_volume="1",
            kafka_random_key="true",
            wait_for_job_completion=True,
        )


@when("I query for the first new claimant from claimant API '{version}'")
@then("I query for the first new claimant from claimant API '{version}'")
def step_impl(context, version):
    api_path = context.ucfs_claimant_api_path_v2_get_award_details

    if version.lower() == "v1":
        api_path = context.ucfs_claimant_api_path_v1_get_award_details

    (
        context.claimant_api_status_code,
        context.claimant_api_response,
    ) = ucfs_claimant_api_helper.query_for_claimant_from_claimant_api(
        context.ucfs_claimant_domain_name,
        context.claimant_api_business_region,
        api_path,
        ucfs_claimant_api_helper.hash_nino(
            context.generated_ninos[0], context.nino_salt
        ),
        context.test_run_name,
    )

    console_printer.print_info(
        f"Query status code is '{context.claimant_api_status_code}' and response is '{context.claimant_api_response}'"
    )


@when("I query for a claimant from claimant API '{version}' who does not exist")
def step_impl(context, version):
    api_path = context.ucfs_claimant_api_path_v2_get_award_details

    if version.lower() == "v1":
        api_path = context.ucfs_claimant_api_path_v1_get_award_details

    (
        context.claimant_api_status_code,
        context.claimant_api_response,
    ) = ucfs_claimant_api_helper.query_for_claimant_from_claimant_api(
        context.ucfs_claimant_domain_name,
        context.claimant_api_business_region,
        api_path,
        "test_unhashed_fake_nino",
        context.test_run_name,
    )

    console_printer.print_info(
        f"Query status code is '{context.claimant_api_status_code}' and response is '{context.claimant_api_response}'"
    )


@given("The new claimants can be found from claimant API '{version}'")
def step_impl(context, version):
    console_printer.print_info(
        f"Waiting for '{len(context.generated_ninos)}' new claimants to be found"
    )

    api_path = context.ucfs_claimant_api_path_v2_get_award_details

    if version.lower() == "v1":
        api_path = context.ucfs_claimant_api_path_v1_get_award_details

    found_ninos = []
    time_taken = 1
    timeout_time = time.time() + context.timeout
    claimants_found = False

    while not claimants_found and time.time() < timeout_time:
        for nino in context.generated_ninos:
            (
                context.claimant_api_status_code,
                context.claimant_api_response,
            ) = ucfs_claimant_api_helper.query_for_claimant_from_claimant_api(
                context.ucfs_claimant_domain_name,
                context.claimant_api_business_region,
                api_path,
                ucfs_claimant_api_helper.hash_nino(nino, context.nino_salt),
                context.test_run_name,
            )
            if (
                context.claimant_api_status_code == 200
                and "claimantFound" in context.claimant_api_response
                and context.claimant_api_response["claimantFound"]
            ):
                found_ninos.append(nino)
                console_printer.print_info(
                    f"Successfully found claimant with nino of '{nino}'"
                )

        if set(found_ninos) == set(context.generated_ninos):
            console_printer.print_info(f"Successfully found all new claimants")
            claimants_found = True

        time.sleep(1)
        time_taken += 1

    assert claimants_found, f"All claimants were not found"


@when("The query succeeds and returns that the claimant has been found")
@then("The query succeeds and returns that the claimant has been found")
def step_impl(context):
    assert (
        context.claimant_api_status_code == 200
    ), f"Status code from response was {context.claimant_api_status_code}, not 200"
    assert context.claimant_api_response is not None, f"Response body was empty"
    assert (
        "claimantFound" in context.claimant_api_response
    ), f"claimantFound not present in response body"
    assert (
        context.claimant_api_response["claimantFound"] == True
    ), f"claimantFound was not set to True"


@then("The query succeeds and returns that the claimant has not been found")
def step_impl(context):
    assert (
        context.claimant_api_status_code == 200
    ), f"Status code from response was {context.claimant_api_status_code}, not 200"
    assert context.claimant_api_response is not None, f"Response body was empty"
    assert (
        "claimantFound" in context.claimant_api_response
    ), f"claimantFound not present in response body"
    assert (
        context.claimant_api_response["claimantFound"] == False
    ), f"claimantFound was not set to False"


@when("The query succeeds and returns that the claimant is not suspended")
@then("The query succeeds and returns that the claimant is not suspended")
def step_impl(context):
    assert (
        context.claimant_api_status_code == 200
    ), f"Status code from response was {context.claimant_api_status_code}, not 200"
    assert context.claimant_api_response is not None, f"Response body was empty"
    assert (
        "suspendedDate" not in context.claimant_api_response
    ), f"suspendedDate not present in response body"


@then(
    "I query the first claimant again from claimant API '{version}' and it is not found"
)
def step_impl(context, version):
    nino = context.generated_ninos[0]
    console_printer.print_info(f"Waiting for '{nino}' claimant to be not found")

    api_path = context.ucfs_claimant_api_path_v2_get_award_details

    if version.lower() == "v1":
        api_path = context.ucfs_claimant_api_path_v1_get_award_details

    time_taken = 1
    timeout_time = time.time() + context.timeout
    claimant_not_found = False

    while not claimant_not_found and time.time() < timeout_time:
        (
            context.claimant_api_status_code,
            context.claimant_api_response,
        ) = ucfs_claimant_api_helper.query_for_claimant_from_claimant_api(
            context.ucfs_claimant_domain_name,
            context.claimant_api_business_region,
            api_path,
            ucfs_claimant_api_helper.hash_nino(nino, context.nino_salt),
            context.test_run_name,
        )
        if (
            context.claimant_api_status_code == 200
            and "claimantFound" in context.claimant_api_response
            and not context.claimant_api_response["claimantFound"]
        ):
            console_printer.print_info(
                f"Successfully retrieved the response and claimant is not found"
            )
            claimant_not_found = True

        time.sleep(1)
        time_taken += 1

    assert claimant_not_found, f"claimantFound was set to True"


@then(
    "I query the first claimant again from claimant API '{version}' and it is suspended"
)
def step_impl(context, version):
    nino = context.generated_ninos[0]
    console_printer.print_info(f"Waiting for '{nino}' claimant to be suspended")

    api_path = context.ucfs_claimant_api_path_v2_get_award_details

    if version.lower() == "v1":
        api_path = context.ucfs_claimant_api_path_v1_get_award_details

    time_taken = 1
    timeout_time = time.time() + context.timeout
    claimant_suspended = False

    while not claimant_suspended and time.time() < timeout_time:
        (
            context.claimant_api_status_code,
            context.claimant_api_response,
        ) = ucfs_claimant_api_helper.query_for_claimant_from_claimant_api(
            context.ucfs_claimant_domain_name,
            context.claimant_api_business_region,
            api_path,
            ucfs_claimant_api_helper.hash_nino(nino, context.nino_salt),
            context.test_run_name,
        )
        if (
            context.claimant_api_status_code == 200
            and "claimantFound" in context.claimant_api_response
            and context.claimant_api_response["claimantFound"]
        ):
            if (
                "suspendedDate" in context.claimant_api_response
                and context.claimant_api_response["suspendedDate"]
            ):
                console_printer.print_info(
                    f"Successfully found claimant and they are suspended"
                )
                claimant_suspended = True

        time.sleep(1)
        time_taken += 1

    assert claimant_suspended, f"suspendedDate could not be found or was False"


@then(
    "The assessment periods are correctly returned using data file of '{data_file_name}'"
)
def step_impl(context, data_file_name):
    global message_type

    assert (
        context.claimant_api_status_code == 200
    ), f"Status code from response was {context.claimant_api_status_code}, not 200"
    assert context.claimant_api_response is not None, f"Response body was empty"

    response = context.claimant_api_response

    folder = streaming_data_helper.generate_fixture_data_folder(message_type)
    expected_assessment_periods = (
        ucfs_claimant_api_helper.retrieve_assessment_periods_from_claimant_data_file(
            input_data_file_name=data_file_name,
            fixture_files_root=context.fixture_path_local,
            fixture_data_folder=folder,
        )
    )

    try:
        actual_assessment_periods = response["assessmentPeriod"]
    except Exception as ex:
        console_printer.print_error_text(
            f"Could not retrieve assessment periods from claimant API response of '{response}' and error of '{ex}'"
        )
        raise ex

    console_printer.print_info(
        f"Successfully retrieved '{len(actual_assessment_periods)}' actual assessment periods"
    )

    assert len(actual_assessment_periods) == len(
        expected_assessment_periods
    ), f"Expected ssessment period count does not match actual count"


@then("The messages are sent to the DLQ")
def step_impl(context):
    expected_dlq_ids = context.generated_ids
    console_printer.print_info(
        f"Found '{len(expected_dlq_ids)}' expected DLQ ids of '{expected_dlq_ids}'"
    )

    time_taken = 1
    timeout_time = time.time() + context.timeout

    while time.time() < timeout_time:
        actual_dlq_files_content_for_today = aws_helper.retrieve_files_from_s3(
            s3_bucket=context.s3_ingest_bucket,
            path=context.s3_dlq_path_and_date_prefix,
            pattern=None,
            remove_whitespace=True,
        )

        console_printer.print_info(
            f"Found '{len(actual_dlq_files_content_for_today)}' actual DLQ files in s3 folder with prefix of '{context.s3_dlq_path_and_date_prefix}'"
        )

        ids_found = 0
        for expected_dlq_id in expected_dlq_ids:
            for actual_dlq_file_content_for_today in actual_dlq_files_content_for_today:
                if str(expected_dlq_id) in actual_dlq_file_content_for_today:
                    ids_found += 1
                    console_printer.print_info(
                        f"Successfully found {ids_found} DLQ files"
                    )
                    if ids_found == len(expected_dlq_ids):
                        console_printer.print_info(f"Successfully found all DLQ files")
                        return

        time.sleep(1)
        time_taken += 1

    raise AssertionError("Could not find DLQ files within timeout")


@when("I print out the NINO for manual regression testing usage")
@then("I print out the NINO for manual regression testing usage")
def step_impl(context):
    console_printer.print_info(f"NINO: '{context.generated_ninos}'")


@then("I clean up the '{temp_file_name}' temporary files")
def step_impl(context, temp_file_name):
    folder = streaming_data_helper.generate_fixture_data_folder(message_type)
    fixture_data_path = os.path.join(context.fixture_path_local, folder)
    temporary_file_full_path = os.path.join(fixture_data_path, temp_file_name)
    file_helper.delete_local_file(temporary_file_full_path)


@when(
    "UCFS send claimant API kafka messages with input file of '{input_file_name}' and data file of '{data_file_name}'"
)
def step_impl(context, input_file_name, data_file_name):
    context.execute_steps(
        f"given UCFS send claimant API kafka messages with input file of '{input_file_name}' and data file of '{data_file_name}'"
    )


@when("The new claimants can be found from claimant API '{api_version}'")
def step_impl(context, api_version):
    context.execute_steps(
        f"given The new claimants can be found from claimant API '{api_version}'"
    )
