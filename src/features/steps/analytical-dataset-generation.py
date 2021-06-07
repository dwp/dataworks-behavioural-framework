from behave import given, then
import uuid
import os
import json
from io import StringIO
import csv
import base64
from helpers import (
    snapshot_data_generator,
    historic_data_load_generator,
    aws_helper,
    invoke_lambda,
    console_printer,
    emr_step_generator,
    file_helper,
    export_status_helper,
    data_pipeline_metadata_helper,
)
from datetime import datetime

PART_FILE_REGEX = r".*part.*"
COMPLETED_STATUS = "COMPLETED"
CLUSTER_ARN = "ClusterArn"
IV = "iv"
DATAENCRYPTIONKEYID = "datakeyencryptionkeyid"
CIPHERTEXT = "ciphertext"
TOPIC = "topic"
RUN_TYPE = "E2E_IMPORT"
TIMESTAMP = "2018-11-01T03:02:01.001Z"
CORRELATION_ID = "correlation_id"
CORRELATION_ID_VALUE = "e2e_test"
S3_PREFIX = "s3_prefix"
SNAPSHOT_TYPE = "snapshot_type"
EXPORT_DATE = "export_date"
ADG_FULL_TOPICS = [
    "db.agent-core.agent",
    "db.agent-core.agentToDo",
    "db.agent-core.team",
    "db.core.statement",
    "db.core.contract",
    "db.core.claimant",
    "db.core.claimantCommitment",
    "db.core.toDo",
    "db.accepted-data.personDetails",
    "db.appointments.appointment",
]

ADG_INCREMENTAL_TOPICS = [
    "db.agent-core.agent",
    "db.agent-core.agentToDo",
    "db.agent-core.team",
    "db.core.statement",
    "db.core.contract",
    "db.core.claimant",
    "db.core.claimantCommitment",
    "db.core.toDo",
    "db.accepted-data.personDetails",
    "db.appointments.appointment",
    "data.businessAudit",
]

ADG_DB_COLLECTION = {
    "agent-core": ["agent", "agentToDo", "team"],
    "core": ["statement", "contract", "claimant", "claimantCommitment", "toDo"],
    "accepted-data": ["personDetails"],
    "appointments": ["appointment"],
}

ADG_INCREMENTAL_TOPICS_DATED = [
    "db.agent-core.agent",
    "db.agent-core.agentToDo",
    "db.agent-core.team",
    "db.core.statement",
    "db.core.contract",
    "db.core.claimant",
    "db.core.claimantCommitment",
    "db.core.toDo",
    "db.accepted-data.personDetails",
    "db.appointments.appointment",
]


@given(
    "the data of the format in the template file '{template_name}' for '{snapshot_type}' as an input to analytical data set generation emr"
)
def step_(context, template_name, snapshot_type):

    if snapshot_type == "full":
        ADG_TOPICS = ADG_FULL_TOPICS
    else:
        ADG_TOPICS = ADG_INCREMENTAL_TOPICS

    for topic in ADG_TOPICS:
        snapshot_local_file = (
            snapshot_data_generator.generate_hbase_record_for_snapshot_file(
                template_name,
                TIMESTAMP,
                uuid.uuid4(),
                RUN_TYPE,
                context.test_run_name,
                topic,
                context.fixture_path_local,
                context.snapshot_files_hbase_records_temp_folder,
                True,
            )
        )
        with open(snapshot_local_file, "r") as unencrypted_file:
            unencrypted_content = unencrypted_file.read()
        [
            iv_int,
            iv_whole,
        ] = historic_data_load_generator.generate_initialisation_vector()
        iv = base64.b64encode(iv_int).decode()
        compressed_encrypted_content = (
            historic_data_load_generator.generate_encrypted_record(
                iv_whole, unencrypted_content, context.encryption_plaintext_key, True
            )
        )
        file_name = os.path.basename(snapshot_local_file)
        s3_prefix = os.path.join(context.mongo_snapshot_path, context.test_run_name)
        s3_key = os.path.join(s3_prefix, file_name)
        metadata = {
            CIPHERTEXT: context.encryption_encrypted_key,
            DATAENCRYPTIONKEYID: context.encryption_master_key_id,
            IV: iv,
        }
        aws_helper.put_object_in_s3_with_metadata(
            compressed_encrypted_content,
            context.mongo_snapshot_bucket,
            s3_key,
            metadata,
        )


@then("start adg '{snapshot_type}' cluster and wait for the step '{step_name}'")
def step_(context, snapshot_type, step_name):
    context.adg_s3_prefix = os.path.join(
        context.mongo_snapshot_path, context.test_run_name
    )
    context.adg_export_date = datetime.now().strftime("%Y-%m-%d")
    payload = {
        CORRELATION_ID: context.test_run_name,
        S3_PREFIX: context.adg_s3_prefix,
        SNAPSHOT_TYPE: snapshot_type,
        EXPORT_DATE: context.adg_export_date,
    }
    payload_json = json.dumps(payload)
    cluster_response = invoke_lambda.invoke_adg_emr_launcher_lambda(payload_json)
    cluster_arn = cluster_response[CLUSTER_ARN]
    cluster_arn_arr = cluster_arn.split(":")
    cluster_identifier = cluster_arn_arr[len(cluster_arn_arr) - 1]
    cluster_identifier_arr = cluster_identifier.split("/")
    cluster_id = cluster_identifier_arr[len(cluster_identifier_arr) - 1]
    console_printer.print_info(f"Started emr cluster : '{cluster_id}'")
    step = aws_helper.get_emr_cluster_step(step_name, cluster_id)
    context.adg_cluster_id = cluster_id
    step_id = step["Id"]
    console_printer.print_info(f"Step id for '{step_name}' : '{step_id}'")
    if step is not None:
        execution_state = aws_helper.poll_emr_cluster_step_status(
            step_id, cluster_id, 2500
        )
        if execution_state != COMPLETED_STATUS:
            raise AssertionError(
                f"'{step_name}' step failed with final status of '{execution_state}'"
            )


@then("insert the '{step_name}' step onto the cluster")
def step_impl(context, step_name):
    context.adg_cluster_step_name = step_name
    file_name = f"{context.test_run_name}.csv"
    adg_hive_export_bash_command = (
        f"hive -e 'SELECT * FROM uc_mongo_latest.statement_fact_v;' >> ~/{file_name} && "
        + f"aws s3 cp ~/{file_name} s3://{context.published_bucket}/{context.mongo_latest_test_query_output_folder}/"
        + f" &>> /var/log/adg/e2e.log"
    )

    context.adg_cluster_step_id = emr_step_generator.generate_bash_step(
        context.adg_cluster_id,
        adg_hive_export_bash_command,
        context.adg_cluster_step_name,
    )
    context.adg_results_s3_file = os.path.join(
        context.mongo_latest_test_query_output_folder, file_name
    )


@then("insert the dynamodb check query step onto the cluster")
def step_impl(context):
    context.adg_ddb_cluster_step_name = "dynamodb_check_query"
    file_name = f"{context.test_run_name}_ddb.csv"
    adg_hive_export_bash_command = (
        f"""hive -e "USE AUDIT; SHOW TABLES LIKE 'data_pipeline_metadata_hive';" >> ~/{file_name} && """
        + f"aws s3 cp ~/{file_name} s3://{context.published_bucket}/{context.mongo_latest_test_query_output_folder}/"
        + f" &>> /var/log/adg/e2e.log"
    )

    context.adg_ddb_cluster_step_id = emr_step_generator.generate_bash_step(
        context.adg_cluster_id,
        adg_hive_export_bash_command,
        context.adg_ddb_cluster_step_name,
    )
    context.adg_ddb_results_s3_file = os.path.join(
        context.mongo_latest_test_query_output_folder, file_name
    )


@then("wait a maximum of '{timeout_mins}' minutes for the last step to finish")
def step_impl(context, timeout_mins):
    timeout_secs = int(timeout_mins) * 60
    execution_state = aws_helper.poll_emr_cluster_step_status(
        context.adg_ddb_cluster_step_id, context.adg_cluster_id, timeout_secs
    )

    if execution_state != "COMPLETED":
        raise AssertionError(
            f"'{context.adg_cluster_step_name}' step failed with final status of '{execution_state}'"
        )


@then(
    "the Mongo-Latest result for step '{step_name}' matches the expected results of '{expected_result_file_name}'"
)
def step_(context, expected_result_file_name, step_name):
    remote_file = (
        context.adg_results_s3_file
        if step_name == "hive-query"
        else context.adg_ddb_results_s3_file
    )
    console_printer.print_info(f"S3 Request Location: {remote_file}")
    actual = (
        aws_helper.get_s3_object(None, context.published_bucket, remote_file)
        .decode("ascii")
        .replace("\t", "")
        .replace(" ", "")
        .strip()
    )

    expected_file_name = os.path.join(
        context.fixture_path_local,
        "mongo_latest",
        "expected",
        expected_result_file_name,
    )
    expected = (
        file_helper.get_contents_of_file(expected_file_name, False)
        .replace("\t", "")
        .replace(" ", "")
        .strip()
    )

    assert (
        expected == actual
    ), f"Expected result of '{expected}', does not match '{actual}'"


@then("read metadata of the analytical data sets from the path '{metadata_path}'")
def step_analytical_datasets_metadata(context, metadata_path):
    content = aws_helper.retrieve_files_from_s3(context.published_bucket, metadata_path)
    s3_file = StringIO(content[0])
    reader = csv.reader(s3_file)
    next(reader)
    for row in reader:
        data_path = row[1]
        context.data_path = data_path
    console_printer.print_info(f"Processed data location : '{context.data_path}'")


@then("verify metadata, tags of the analytical data sets for '{snapshot_type}'")
def step_verify_analytical_datasets(context, snapshot_type):
    keys = aws_helper.get_s3_file_object_keys_matching_pattern(
        context.published_bucket, context.data_path, PART_FILE_REGEX
    )
    console_printer.print_info(f"Keys in data location : {keys}")
    if snapshot_type == "full":
        ADG_TOPICS = ADG_FULL_TOPICS
    else:
        ADG_TOPICS = ADG_INCREMENTAL_TOPICS_DATED
    console_printer.print_info(f"keys are : {keys}")
    console_printer.print_info(f"ADG_TOPICS are : {ADG_TOPICS}")
    assert len(keys) == len(ADG_TOPICS)
    for ADG_DB, collections in ADG_DB_COLLECTION.items():
        for collection in collections:
            part_file_key = f"{context.data_path}/{ADG_DB}/{collection}/part-00000.lzo"
            assert part_file_key in keys
            tags = aws_helper.get_tags_of_file_in_s3(
                context.published_bucket, part_file_key
            )["TagSet"]
            console_printer.print_info(f"Tags are : {tags}")
            found_tag_count = 0
            for tag in tags:
                key = tag["Key"]
                value = tag["Value"]
                if key == "pii":
                    found_tag_count += 1
                    assert value == "true", f"PII tag value is '{value}' and not 'true'"
                if key == "db":
                    found_tag_count += 1
                    assert (
                        value == ADG_DB
                    ), f"DB tag value is '{value}' and not '{ADG_DB}'"
                if key == "table":
                    found_tag_count += 1
                    assert (
                        value == collection
                    ), f"Table tag value is '{value}' and not '{collection}'"
                if key == "snapshot_type":
                    found_tag_count += 1
                    assert (
                        value == snapshot_type
                    ), f"Snapshot type tag value is '{value}' and not '{snapshot_type}'"

            assert found_tag_count == 4, f"One or more tags not found"

            metadata = aws_helper.get_s3_object_metadata(
                context.published_bucket, part_file_key
            )
            console_printer.print_info(f"metadata : {metadata}")
            assert "x-amz-iv" in metadata
            assert "x-amz-key" in metadata
            assert "x-amz-matdesc" in metadata


@then("the ADG cluster tags have been created correctly for '{snapshot_type}'")
def step_check_adg_cluster_tags(context, snapshot_type):
    console_printer.print_info(f"Checking cluster Tags")
    cluster_id = context.adg_cluster_id
    console_printer.print_info(f"Cluster id : {cluster_id}")
    cluster_tags = aws_helper.check_tags_of_cluster(cluster_id)
    console_printer.print_info(f"Cluster tags : {cluster_tags}")
    tags_to_check = {"Key": "Correlation_Id", "Value": context.test_run_name}
    console_printer.print_info(f"Tags to check : {tags_to_check}")

    assert tags_to_check in cluster_tags


@then("the ADG metadata table is correct for '{snapshot_type}'")
def metadata_table_step_impl(context, snapshot_type):
    data_product = f"ADG-{snapshot_type.lower()}"

    response = data_pipeline_metadata_helper.get_item_from_product_status_table(
        context.dynamo_db_product_status_table_name,
        data_product,
        context.test_run_name,
    )

    console_printer.print_info(
        f"Data retrieved from product status table : '{response}'"
    )

    assert (
        "Item" in response
    ), f"Could not find metadata table row with correlation id of '{context.test_run_name}' and data product  of '{data_product}'"

    item = response["Item"]
    console_printer.print_info(f"Item retrieved from dynamodb table : '{item}'")

    allowed_steps = [
        "spark-submit",
        "create_pdm_trigger",
        "flush-pushgateway",
        "send_notification",
    ]

    if snapshot_type.lower() == "incremental":
        allowed_steps = [
            "create_pdm_trigger",
            "flush-pushgateway",
            "executeUpdateAll",
            "bash",
        ]

    assert item["TimeToExist"]["N"] is not None, f"Time to exist was not set"
    assert (
        item["Run_Id"]["N"] == "1"
    ), f"Run_Id was '{item['Run_Id']['N']}', expected '1'"
    assert (
        item["Date"]["S"] == context.adg_export_date
    ), f"Date was '{item['Date']['S']}', expected '{context.adg_export_date}'"
    assert (
        item["CurrentStep"]["S"] in allowed_steps
    ), f"CurrentStep was '{item['CurrentStep']['S']}', expected one of '{allowed_steps}'"
    assert (
        item["Cluster_Id"]["S"] == context.adg_cluster_id
    ), f"Cluster_Id was '{item['Cluster_Id']['S']}', expected '{context.adg_cluster_id}'"
    assert (
        item["S3_Prefix_Snapshots"]["S"] == context.adg_s3_prefix
    ), f"S3_Prefix_Snapshots was '{item['S3_Prefix_Snapshots']['S']}', expected '{context.adg_s3_prefix}'"
    assert (
        item["Snapshot_Type"]["S"] == snapshot_type
    ), f"Snapshot_Type was '{item['Snapshot_Type']['S']}', expected '{snapshot_type}'"


@then(
    "The dynamodb status for each collection for '{snapshot_type}' is set to '{expected}'"
)
def step_impl(context, snapshot_type, expected):
    if snapshot_type == "full":
        ADG_TOPICS = ADG_FULL_TOPICS
    else:
        ADG_TOPICS = ADG_INCREMENTAL_TOPICS_DATED

    for topic in ADG_TOPICS:
        response = export_status_helper.get_item_from_export_status_table(
            context.dynamo_db_export_status_table_name,
            topic,
            context.test_run_name,
        )

        assert (
            response is not None
        ), f"Could not retrieve status row for topic '{topic}' and correlation id '{context.test_run_name}'"
        assert (
            "Item" in response
        ), f"Could not retrieve status row item for topic '{topic}' and correlation id '{context.test_run_name}'"

        item = response["Item"]
        assert (
            "ADGStatus" in item
        ), f"Could not retrieve status dynamodb from item '{item}'"

        actual = item["ADGStatus"]["S"]
        assert (
            expected == actual
        ), f"Actual status of '{actual}' is not the same as the expected status of '{expected}'"
