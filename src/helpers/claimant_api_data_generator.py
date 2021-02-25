import os
import uuid
import yaml
import json
from uuid import UUID
from datetime import datetime, timedelta
from helpers import (
    console_printer,
    date_helper,
    file_helper,
    aws_helper,
    ucfs_claimant_api_helper,
    template_helper,
)


_base_datetime_timestamp = datetime.strptime(
    "2017-11-01T03:02:01.001", "%Y-%m-%dT%H:%M:%S.%f"
)
time_stamp_format = "%Y-%m-%dT%H:%M:%S.%fZ"


def generate_claimant_api_kafka_files(
    s3_input_bucket,
    input_data_file_name,
    input_template_name,
    new_uuid,
    local_files_temp_folder,
    fixture_files_root,
    s3_output_prefix,
    seconds_timeout,
    fixture_data_folder,
):
    """Returns array of generated kafka data as tuples of (input s3 location, output local file).

    Keyword arguments:
    s3_input_bucket - the bucket for the remote fixture files
    input_data_file_name -- the input file name containing the data
    input_template_name -- the name of the input template file
    new_uuid -- the uuid to use for the id (None to generate one for each file)
    local_files_temp_folder -- the root folder for the temporary files to sit in
    fixture_files_root -- the local path to the feature file to send
    s3_output_prefix -- the output path for the edited file in s3
    seconds_timeout -- the timeout in seconds for the test
    fixture_data_folder -- the folder from the root of the fixture data
    """
    global _base_datetime_timestamp

    console_printer.print_info(
        f"Generating UCFS claimant API Kafka files "
        + f"using input data file of '{input_data_file_name}', input template of '{input_template_name}', "
        + f"id of '{new_uuid}' and base timestamp of '{_base_datetime_timestamp}'"
    )

    data_file_name = os.path.join(fixture_files_root, fixture_data_folder, input_data_file_name)
    input_data = yaml.safe_load(open(data_file_name))

    claimant_file_data = []
    contract_file_data = []
    statement_file_data = []
    all_ninos = []
    all_ids = []

    increment = 0
    for item in input_data:
        console_printer.print_info(
            f"Generating claimant data for item of '{item}'"
        )
        contract_id = new_uuid if new_uuid is not None else uuid.uuid4()
        citizen_id = new_uuid if new_uuid is not None else uuid.uuid4()
        person_id = new_uuid if new_uuid is not None else uuid.uuid4()

        (timestamp, timestamp_string) = date_helper.add_milliseconds_to_timestamp(
            _base_datetime_timestamp, increment, True
        )

        if "count" in item:
            count = 0
            while count < item['count']:
                nino = generate_national_insurance_number(citizen_id)
                unique_suffix = f"{increment}{count}"
                claimant_db_object = _generate_claimant_db_object(citizen_id, person_id, nino, unique_suffix)
                all_ninos.append(nino)
                (contract_db_object, statement_db_objects_array) = _generate_contract_and_statement_db_objects(
                    contract_id, item, [citizen_id], unique_suffix, timestamp_string
                )

                claimant_file_data.append((citizen_id, timestamp_string, claimant_db_object))
                all_ids.append(citizen_id)
                contract_file_data.append((contract_id, timestamp_string, contract_db_object))
                all_ids.append(contract_id)
                statement_file_data.extend([(statement_id, timestamp_string, statement_db_object) for (statement_id, statement_db_object) in statement_db_objects_array])
                all_ids.extend([statement_id for (statement_id, statement_db_object) in statement_db_objects_array])

                count += 1
        else:
            nino = generate_national_insurance_number(citizen_id)
            claimant_db_objects_array = [(citizen_id, _generate_claimant_db_object(citizen_id, person_id, nino, increment))]
            all_ninos.append(nino)
            if "partner_nino" in item:
                citizen_id = new_uuid if new_uuid is not None else uuid.uuid4()
                person_id = new_uuid if new_uuid is not None else uuid.uuid4()
                partner_nino = generate_national_insurance_number(citizen_id)
                increment += 1
                claimant_db_objects_array.append((citizen_id, _generate_claimant_db_object(citizen_id, person_id, partner_nino, increment)))
                all_ninos.append(partner_nino)
            
            citizen_ids_array = [citizen_id for (citizen_id, claimant_db_object) in claimant_db_objects_array]
            (contract_db_object, statement_db_objects_array) = _generate_contract_and_statement_db_objects(
                contract_id, item, citizen_ids_array, increment, timestamp_string
            )

            claimant_file_data.extend([(citizen_id, timestamp_string, claimant_db_object) for (citizen_id, claimant_db_object) in claimant_db_objects_array])
            all_ids.extend([citizen_id for (citizen_id, claimant_db_object) in claimant_db_objects_array])
            contract_file_data.append((contract_id, timestamp_string, contract_db_object))
            all_ids.append(contract_id)
            statement_file_data.extend([(statement_id, timestamp_string, statement_db_object) for (statement_id, statement_db_object) in statement_db_objects_array])
            all_ids.extend([statement_id for (statement_id, statement_db_object) in statement_db_objects_array])

        increment += 1

    kafka_input_file_data = [
        ("citizenId", claimant_file_data),
        ("contractId", contract_file_data),
        ("statementId", statement_file_data)
    ]

    return_data = generate_return_data(
        kafka_input_file_data, 
        input_template_name, 
        s3_input_bucket, 
        fixture_data_folder, 
        local_files_temp_folder,
        fixture_files_root,
        s3_output_prefix,
        seconds_timeout,
    )

    return (return_data, all_ninos, all_ids)


def generate_return_data(
    kafka_input_file_data, 
    input_template_name, 
    s3_input_bucket, 
    fixture_data_folder, 
    local_files_temp_folder,
    fixture_files_root,
    s3_output_prefix,
    seconds_timeout,
):
    """Generates the file data to be returned.

    Keyword Arguments:
    kafka_input_file_data -- the generated kafka files as an array of tuples of (id_field, objects_array)
    input_template_name -- the input template name for the files
    s3_input_bucket - the bucket for the remote fixture files
    fixture_data_folder -- the folder from the root of the fixture data
    local_files_temp_folder -- the root folder for the temporary files to sit in
    fixture_files_root -- the local path to the feature file to send
    s3_output_prefix -- the output path for the edited file in s3
    seconds_timeout -- the timeout in seconds for the test
    """
    return_data = []

    for (id_field_name, db_object_array) in kafka_input_file_data:
        count = 0
        generated_files = []
        for (key, file_timestamp, db_object) in db_object_array:
            count += 1
            local_file_name = f"{id_field_name.lower()}_{count}_{input_template_name}"
            generated_files.append(
                _generate_kafka_file(
                    s3_input_bucket,
                    fixture_data_folder,
                    input_template_name,
                    local_file_name,
                    key,
                    id_field_name,
                    local_files_temp_folder,
                    fixture_files_root,
                    s3_output_prefix,
                    seconds_timeout,
                    db_object,
                    file_timestamp,
                )
            )
        return_data.append((id_field_name, generated_files))
    
    return return_data


def _generate_kafka_file(
    s3_input_bucket,
    local_folder,
    input_template_name,
    local_file_name,
    new_uuid,
    id_field_name,
    local_files_temp_folder,
    fixture_files_root,
    s3_output_prefix,
    seconds_timeout,
    db_object,
    file_timestamp,
):
    """Generates a Kafka input file and sends it to S3 and returns remote location.

    Keyword arguments:
    s3_input_bucket - the bucket for the remote fixture files
    local_folder -- the local parent folder for the template file
    input_template_name -- the input template to use locally
    local_file_name -- the file name to use locally
    new_uuid -- the uuid to use for the id
    id_field_name -- the field name for the id field
    local_files_temp_folder -- the root folder for the temporary files to sit in
    fixture_files_root -- the local path to the feature file to send
    s3_output_prefix -- the output path for the edited file in s3
    seconds_timeout -- the timeout in seconds for the test
    db_object -- the db object for the test
    file_timestamp -- the timestamp in the file
    """
    console_printer.print_info(
        f"Generating local Kafka file with name of '{local_file_name}', template of '{input_template_name}', id of '{new_uuid}' and timestamp of '{file_timestamp}'"
    )

    file_name = os.path.join(fixture_files_root, local_folder, input_template_name)

    with open(file_name) as open_file:
        record = open_file.read()

    topic_name = ucfs_claimant_api_helper.get_topic_by_id_type(id_field_name)
    (database, collection) = template_helper.get_database_and_collection_from_topic_name(topic_name)

    record = record.replace("||newid||", f"{new_uuid}")
    record = record.replace("||id_field_name||", id_field_name)
    record = record.replace("||db_object||", json.dumps(db_object, cls=UUIDEncoder))
    record = record.replace("||timestamp||", f"{file_timestamp}")
    record = record.replace("||db||", database)
    record = record.replace("||collection||", collection)

    output_file_local = file_helper.generate_local_output_file(
        local_folder, local_file_name, local_files_temp_folder
    )

    console_printer.print_info(f"Writing Kafka input file to '{output_file_local}'")
    with open(f"{output_file_local}", "w") as output_file_data:
        output_file_data.write(record)

    output_file_full_path_s3 = os.path.join(s3_output_prefix, str(new_uuid))
    full_file_path_s3 = os.path.join(output_file_full_path_s3, local_file_name)
    aws_helper.upload_file_to_s3_and_wait_for_consistency(
        output_file_local, s3_input_bucket, seconds_timeout, full_file_path_s3
    )

    return (full_file_path_s3, output_file_local)


def _month_delta(date, delta):
    """
    Generate a date from another date and month offset
    Examples:
        last_month = _month_delta(datetime.today(), -1)
        next_month = _month_delta(datetime.today(), 1)

    Keyword arguments:
    date -- the date to use a datetime
    delta -- the offset as in int
    """
    m, y = (date.month+delta) % 12, date.year + (date.month+delta-1) // 12
    if not m:
        m = 12
    d = min(date.day, [31,
                       29 if y % 4 == 0 and not y % 400 == 0 else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m-1])
    return date.replace(day=d, month=m, year=y)


def _generate_claimant_db_object(citizen_id, person_id, nino, unique_suffix):
    """Generates a claimant db object and returns it as a dict.

    Keyword arguments:
    citizen_id - the uuid for the citizen id
    person_id -- the uuid for the person id
    nino -- the national insurance number
    unique_suffix -- a unique suffix for the first and last names
    """
    global time_stamp_format

    claimant = {
        "_id": {
            "citizenId": str(citizen_id)
        },
        "personId": str(person_id),
        "firstName": f"Joe{unique_suffix}",
        "lastName": f"Bloggs{unique_suffix}",
        "nino": nino,
        "claimantProvidedNino": None,
        "createdDateTime": _month_delta(datetime.today(), -3).strftime(time_stamp_format),
        "_version": 29,
        "hasVerifiedEmailId": True,
        "inactiveAccountEmailReminderSentDate": None,
        "managedJourney": {
            "type": "REG_SINGLE",
            "currentStep": "COMPLETE"
        },
        "anonymous": False,
        "govUkVerifyStatus": "VERIFY_NOT_ATTEMPTED",
        "cisInterestStatus": "NOT_SET",
        "deliveryUnits": [
            "76783bd7-f709-46bf-8ea4-b0379e7ae1d3"
        ],
        "workCoach": {
            "type": "WORK_COACH_NOT_REQUIRED"
        },
        "supportingAgents": [],
        "currentWorkGroupOnCurrentContract": "INTENSIVE",
        "caseManager": None,
        "claimantDeathDetails": None,
        "pilotGroup": None,
        "firstNameCaseInsensitive": "jdata",
        "lastNameCaseInsensitive": "works",
        "_entityVersion": 0,
        "_lastModifiedDateTime": _month_delta(datetime.today(), -1).strftime(time_stamp_format),
        "identityDocumentsStatus": None,
        "bookAppointmentStatus": "ALL_BOOKED",
        "webAnalyticsUserId": None,
        "searchableNames": [
            "data",
            "works"
        ]
    }

    return claimant


def _generate_contract_and_statement_db_objects(contract_id, item, citizen_ids_array, unique_suffix, timestamp_string):
    """Generates contract and statement db objects and returns it them as a tuple of dict and array(dict).

    Keyword arguments:
    contract_id - the uuid for the contract id
    item -- the data item from the data file
    citizen_ids_array -- the citizen ids as an array
    unique_suffix -- a unique suffix for the first and last names
    timestamp_string -- the timestamp for created and modified times
    """
    global time_stamp_format

    payment_day_of_month = 23

    # Note: Date offsets are simply to make data more natural, nothing known to depend on them
    contract = {
        "_id": {
            "contractId": str(contract_id)
        },
        "assessmentPeriods": [],
        "people": [str(citizen_id) for citizen_id in citizen_ids_array],
        "declaredDate": int((_month_delta(datetime.today(), -2) - timedelta(days=7)).strftime("%Y%m%d")),
        "startDate": int((_month_delta(datetime.today(), -2) - timedelta(days=7)).strftime("%Y%m%d")),
        "entitlementDate": int(_month_delta(datetime.today(), -2).strftime("%Y%m%d")),
        "closedDate": int(item['contract_closed_date']) if "contract_closed_date" in item else None,
        "annualVerificationEligibilityDate": None,
        "annualVerificationCompletionDate": None,
        "paymentDayOfMonth": payment_day_of_month,
        "flags": [],
        "claimClosureReason": "FraudIntervention",
        "_version": 12,
        "createdDateTime": timestamp_string,
        "coupleContract": False,
        "claimantsExemptFromWaitingDays": [],
        "contractTypes": None,
        "_entityVersion": 2,
        "_lastModifiedDateTime": timestamp_string,
        "stillSingle": True,
        "contractType": "INITIAL"
    }

    if "suspension_date" in item:
        contract['claimSuspension'] = {
            "suspensionDate": int(item['suspension_date'])
        }
    elif "suspension_date_offset" in item:
        contract['claimSuspension'] = {
            "suspensionDate": int(
                (datetime.today() - timedelta(days=int(item['suspension_date_offset']))).strftime("%Y%m%d"))
        }
    elif unique_suffix % 10 == 0:
        contract['claimSuspension'] = {
            "suspensionDate": None
        }

    statement_db_objects = []

    if 'assessment_periods' in item:
        for assessment_period in item['assessment_periods']:
            assessment_period_id = f"{uuid.uuid4()}"
            end_date = datetime.strptime(assessment_period['end_date'], "%Y%m%d") \
                if 'end_date' in assessment_period else \
                datetime.today() - timedelta(days=int(assessment_period['end_date_offset']))
            start_date = datetime.strptime(assessment_period['start_date'], "%Y%m%d") \
                if 'start_date' in assessment_period else \
                _month_delta(end_date, -1) - timedelta(days=1)
            ap_to_append = {
                "assessmentPeriodId": assessment_period_id,
                "contractId": str(contract_id),
                "startDate": int(start_date.strftime("%Y%m%d")),
                "endDate": int(end_date.strftime("%Y%m%d")),
                "paymentDate": int(end_date.strftime("%Y%m") + str(payment_day_of_month)),
                "processDate": None,
                "createdDateTime": timestamp_string
            }
            contract['assessmentPeriods'].append(ap_to_append)
            statement_db_objects.append(_generate_statement_db_object(
                assessment_period, citizen_ids_array, contract_id, unique_suffix, ap_to_append, timestamp_string))

    return (contract, statement_db_objects)


def _generate_statement_db_object(item, citizen_ids_array, contract_id, unique_suffix, assessment_period, timestamp_string):
    """Generates a statement id and db object and returns them as a tuple of string, dict.

    Keyword arguments:
    item -- the data item from the data file
    citizen_ids_array -- the citizen ids as an array
    contract_id - the uuid for the contract id
    unique_suffix -- a unique suffix for the first and last names
    assessment_period -- the assessment period object
    timestamp_string -- the timestamp for created and modified times
    """
    statement_id = uuid.uuid4()
    crypto_id = uuid.uuid4()

    claimant_data = []
    for offset, citizen_id in enumerate(citizen_ids_array):
        claimant_data.append(
            {
                "type": "person",
                "contractId": str(contract_id),
                "citizenId": str(citizen_id),
                "lastName": f"Bloggs{unique_suffix - (len(citizen_ids_array)-1) + offset}",
                "middleName": None,
                "firstName": f"Jo{unique_suffix - (len(citizen_ids_array)-1) + offset}",
                "email": f"jo{unique_suffix - (len(citizen_ids_array)-1) + offset}\@example.com",
                "mobileNumber": f"07{unique_suffix - (len(citizen_ids_array)-1) + offset}",
                "dateOfBirth": {
                    "type": "PersonDateOfBirth",
                    "cryptoId": str(crypto_id)
                },
                "contactPreference": "MOBILE",
                "gender": "female",
                "verifiedUsingBioQuestionsOrThirdParty": None,
                "effectiveDate": {
                    "type": "FROM_START_OF_CLAIM"
                },
                "paymentEffectiveDate": {
                    "type": "FROM_START_OF_CLAIM"
                },
                "declaredDateTime": "20180510"
            }
        )

    statement = {
        "_id": {
            "statementId": str(statement_id)
        },
        "_version": 1,
        "people": claimant_data,
        "assessmentPeriod": assessment_period,
        "standardAllowanceElement": "317.82",
        "housingElement": "0.00",
        "housingElementRent": "0.00",
        "housingElementServiceCharges": "0.00",
        "childElement": "0.00",
        "numberEligibleChildren": 0,
        "disabledChildElement": "0.00",
        "numberEligibleDisabledChildren": 0,
        "childcareElement": "0.00",
        "numberEligibleChildrenInChildCare": 0,
        "carerElement": "0.00",
        "numberPeopleCaredFor": 0,
        "takeHomePay": item['amount'],
        "takeHomeBreakdown": {
            "rte": "0.00",
            "selfReported": "0.00",
            "selfEmployed": "0.00",
            "selfEmployedWithMif": "0.00"
        },
        "unaffectedPayElement": "0.00",
        "totalReducedForHomePay": "0.00",
        "otherIncomeAdjustment": "0.00",
        "capitalAdjustment": "0.00",
        "totalAdjustments": "0.00",
        "fraudPenalties": "0.00",
        "sanctions": "317.82",
        "advances": "0.00",
        "deductions": "0.00",
        "totalPayment": "0.00",
        "createdDateTime": timestamp_string,
        "earningsSource": None,
        "otherBenefitAwards": [],
        "overlappingBenefits": [],
        "totalOtherBenefitsAdjustment": "0",
        "capApplied": None,
        "type": "CALCULATED",
        "preAdjustmentTotal": "317.82",
        "_entityVersion": 4,
        "_lastModifiedDateTime": timestamp_string,
        "workCapabilityElement": None,
        "benefitCapThreshold": None,
        "benefitCapAdjustment": None,
        "gracePeriodEndDate": None,
        "landlordPayment": "0"
    }

    return (statement_id, statement)


def generate_national_insurance_number(citizen_id):
    """Generates a new national insurance number from given id.

    Keyword arguments:
    citizen_id - the id to use
    """
    return str(citizen_id).replace("-", "").upper()[:9]


def generate_updated_contract_and_statement_files_for_existing_claimant(
    citizen_id, 
    contract_id, 
    fixture_files_root, 
    fixture_data_folder, 
    input_data_file_name,
    input_template_name, 
    s3_input_bucket, 
    local_files_temp_folder,
    s3_output_prefix,
    seconds_timeout,
):
    """Returns an updated contract and statement files according to incoming data for existing claimant.

    Keyword arguments:
    citizen_id - the id of the claimant
    contract_id - the id of the contract
    fixture_files_root -- the local path to the feature file to send
    fixture_data_folder -- the folder from the root of the fixture data
    input_data_file_name -- the input file name containing the data
    input_template_name -- the input template name for the files
    s3_input_bucket - the bucket for the remote fixture files
    local_files_temp_folder -- the root folder for the temporary files to sit in
    s3_output_prefix -- the output path for the edited file in s3
    seconds_timeout -- the timeout in seconds for the test
    """
    global _base_datetime_timestamp

    data_file_name = os.path.join(fixture_files_root, fixture_data_folder, input_data_file_name)
    input_data = yaml.safe_load(open(data_file_name))

    claimant_file_data = []
    contract_file_data = []
    statement_file_data = []
    increment = 0

    for item in input_data:
        (timestamp, timestamp_string) = date_helper.add_milliseconds_to_timestamp(
            _base_datetime_timestamp, increment + 1, True
        )
        if "count" in item:
            count = 0
            while count < item['count']:
                unique_suffix = f"{increment}{count}"
                (contract_db_object, statement_db_objects_array) = _generate_contract_and_statement_db_objects(
                    contract_id, item, [citizen_id], unique_suffix, timestamp_string
                )
                count += 1
            
            contract_file_data.append((contract_id, timestamp_string, contract_db_object))
            statement_file_data.extend([(statement_id, timestamp_string, statement_db_object) for (statement_id, statement_db_object) in statement_db_objects_array])
        else:
            (contract_db_object, statement_db_objects_array) = _generate_contract_and_statement_db_objects(
                contract_id, item, [citizen_id], increment, timestamp_string
            )

            contract_file_data.append((contract_id, timestamp_string, contract_db_object))
            statement_file_data.extend([(statement_id, timestamp_string, statement_db_object) for (statement_id, statement_db_object) in statement_db_objects_array])
        increment += 1

    kafka_input_file_data = [
        ("contractId", contract_file_data),
        ("statementId", statement_file_data)
    ]

    return_data = generate_return_data(
        kafka_input_file_data, 
        input_template_name, 
        s3_input_bucket, 
        fixture_data_folder, 
        local_files_temp_folder,
        fixture_files_root,
        s3_output_prefix,
        seconds_timeout,
    )

    return return_data


def generate_updated_claimant_file_for_existing_claimant(
    citizen_id, 
    person_id, 
    fixture_files_root, 
    fixture_data_folder, 
    input_template_name, 
    s3_input_bucket, 
    local_files_temp_folder,
    s3_output_prefix,
    seconds_timeout,
    increment,
):
    """Returns an updated claimant file according to incoming ids for existing claimant.

    Keyword arguments:
    citizen_id - the id of the claimant
    person_id - the id of the person
    fixture_files_root -- the local path to the feature file to send
    fixture_data_folder -- the folder from the root of the fixture data
    input_template_name -- the input template name for the files
    s3_input_bucket - the bucket for the remote fixture files
    local_files_temp_folder -- the root folder for the temporary files to sit in
    s3_output_prefix -- the output path for the edited file in s3
    seconds_timeout -- the timeout in seconds for the test
    increment -- a unique number for this claimant unique within this specific test context
    """
    global _base_datetime_timestamp

    console_printer.print_info(
        f"Generating claimant data for citizen id of '{citizen_id}'"
    )

    (timestamp, timestamp_string) = date_helper.add_milliseconds_to_timestamp(
        _base_datetime_timestamp, increment, True
    )

    nino = generate_national_insurance_number(citizen_id)
    claimant_db_object = _generate_claimant_db_object(citizen_id, person_id, nino, increment)
    claimant_file_data = [(citizen_id, timestamp_string, claimant_db_object)]

    kafka_input_file_data = [
        ("citizenId", claimant_file_data)
    ]

    return_data = generate_return_data(
        kafka_input_file_data, 
        input_template_name, 
        s3_input_bucket, 
        fixture_data_folder, 
        local_files_temp_folder,
        fixture_files_root,
        s3_output_prefix,
        seconds_timeout,
    )

    return return_data


class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UUID):
            # if the obj is uuid, we simply return the value of uuid
            return obj.hex
        return json.JSONEncoder.default(self, obj)
