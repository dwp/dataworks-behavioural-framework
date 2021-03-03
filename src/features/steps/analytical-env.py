import os
import json
import copy
from behave import given, when
from helpers import (
    aws_helper,
    historic_data_generator,
    template_helper,
    file_helper,
    file_comparer,
    console_printer,
    analytical_env_helper,
)

access_denied_message = "Input path does not exist"


@when("The analytical environment is setup")
def step_impl(context):
    console_printer.print_info("Analytical environment setup")


@given("S3 file exists")
def step_create_file_in_s3_location(context):
    path = context.analytical_test_data_s3_location["path"]
    file_name = context.analytical_test_data_s3_location["file_name"]
    context.analytical_test_data_s3_key = os.path.join(path, file_name)
    local_dir = "/tmp/"

    file_helper.create_local_file(file_name, local_dir)
    aws_helper.upload_file_to_s3_and_wait_for_consistency(
        os.path.join(local_dir, file_name),
        context.published_bucket,
        context.timeout,
        context.analytical_test_data_s3_key,
    )
    file_helper.delete_local_file(file_name, local_dir)


@when("the concourse user attempts to read the file")
def step_read_file_from_s3(context):
    context.read_access = aws_helper.test_s3_access_read(
        context.published_bucket, context.analytical_test_data_s3_key
    )


@then("the user is able to read the file from S3")
def step_user_has_read_access(context):
    assert context.read_access == True

@given(
    "A user is not cleared to access PII data from the UC database in the published S3 bucket"
)
@given("A user is cleared to access PII data in the published S3 bucket")
@given("A user is not cleared to access PII data in the published S3 bucket")
def step_non_sc_role_assumed(context):
    context.analytical_test_data_s3_location["path"] = "data/uc/"

    tag_map = {"pii": "true", "db": "uc", "table": "pii_test_table"}
    setup_test_file_in_s3(context, tag_map)

    assume_role_for_test(context)


@when(
    "The user attempts to read data in the published S3 bucket location"
)
@when(
    "The user Attempts to read data tagged with the pii:false tag from the UC database in the published S3 bucket"
)
@when(
    "The user Attempts to read data tagged with the pii:true tag from the UC database in the published S3 bucket"
)
@when("The user attempts to read data from another table in the published S3 bucket")
@when(
    "The user attempts to read data from the their team table in the published S3 bucket"
)
@when(
    "The user attempts to read data from the auditlog_red_v table in the published S3 bucket"
)
@when(
    "The user attempts to read data from the auditlog_sec_v table in the published S3 bucket"
)
@when(
    "The user attempts to read data from the auditlog_unred_v table in the published S3 bucket"
)
@when(
    "The user Attempts to read data tagged with the pii:false tag in the published S3 bucket"
)
@when(
    "The user Attempts to read data tagged with the pii:true tag in the published S3 bucket"
)
def step_attempt_to_read_data(context):
    context.read_access = aws_helper.test_s3_access_read(
        context.published_bucket,
        os.path.join(
            context.analytical_test_data_s3_location["path"],
            context.analytical_test_data_s3_location["file_name"],
        ),
    )

@then("The user is unable to read the data")
def step_no_read_access(context):
    assert context.read_access is False


@given(
    "A user is not cleared to access non PII data from the UC database in the published S3 bucket"
)
@given("A user is cleared to access non PII data in the published S3 bucket")
def step_non_sc_role_assumed(context):
    context.analytical_test_data_s3_location["path"] = "data/uc/"

    tag_map = {"pii": "false", "db": "uc", "table": "pii_test_table"}
    setup_test_file_in_s3(context, tag_map)

    assume_role_for_test(context)


@then("The user is able to read the data")
def step_read_access(context):
    assert context.read_access is True


@given(
    "A user is not cleared to access any data in any team databases in the published S3 bucket"
)
def step_non_sc_role_assumed(context):
    team_databases = [
        "uc_ris_redacted",
        "uc_ris_unredacted",
        "ucs_opsmi_redacted",
        "ucs_opsmi_unredacted",
    ]
    team_paths = [f"data/{db}/" for db in team_databases]
    context.analytical_test_data_s3_location["paths"] = team_paths

    for db in team_databases:
        tag_map = {"db": db}
        context.analytical_test_data_s3_location["path"] = f"data/{db}/"
        setup_test_file_in_s3(context, tag_map)
        del context.analytical_test_data_s3_location["path"]

    assume_role_for_test(context)


@given(
    "A user is not cleared to access any non PII data in any team databases in the published S3 bucket"
)
def step_non_sc_role_assumed(context):
    team_databases = [
        "uc_ris_redacted",
        "uc_ris_unredacted",
        "ucs_opsmi_redacted",
        "ucs_opsmi_unredacted",
    ]
    team_paths = [f"data/{db}/" for db in team_databases]
    context.analytical_test_data_s3_location["paths"] = team_paths

    for db in team_databases:
        tag_map = {"pii": "false", "db": db, "table": "test_table"}
        context.analytical_test_data_s3_location["path"] = f"data/{db}/"
        setup_test_file_in_s3(context, tag_map)
        del context.analytical_test_data_s3_location["path"]

    assume_role_for_test(context)


@given(
    "A user is not cleared to access data in the restricted directories in the published S3 bucket"
)
def step_non_sc_role_assumed(context):
    restricted_tables = ["auditlog_red_v", "auditlog_unred_v", "auditlog_sec_v"]
    restricted_paths = [f"data/uc/{table}/" for table in restricted_tables]
    context.analytical_test_data_s3_location["paths"] = restricted_paths

    for table in restricted_tables:
        tag_map = {"pii": "true", "db": "uc", "table": table}
        context.analytical_test_data_s3_location["path"] = f"data/uc/{table}/"
        setup_test_file_in_s3(context, tag_map)
        del context.analytical_test_data_s3_location["path"]

    assume_role_for_test(context)


@when(
    "The user Attempts to read data from each of the team directories in the published S3 bucket"
)
@when(
    "The user attempts to read data tagged pii=false from each of the team directories in the published S3 bucket"
)
@when(
    "The user Attempts to read data from each of the restricted directories in the published S3 bucket"
)
def step_attempt_to_read_pii_data(context):
    context.read_access_path_list = []
    for path in context.analytical_test_data_s3_location["paths"]:
        context.read_access_path_list.append(
            aws_helper.test_s3_access_read(
                context.published_bucket,
                os.path.join(
                    path, context.analytical_test_data_s3_location["file_name"]
                ),
            )
        )


@then("The user is unable to read any of the data")
def no_read_access_for_restricted_paths(context):
    for read_access in context.read_access_path_list:
        assert read_access is False


@given(
    "A user is cleared to access data in the auditlog_red_v table in the published S3 bucket"
)
@given(
    "A user is not cleared to access data in the auditlog_red_v table in the published S3 bucket"
)
def step_auditlog_red_v_upload(context):
    context.analytical_test_data_s3_location["path"] = "data/uc/auditlog_red_v/"

    tag_map = {"pii": "true", "db": "uc", "table": "auditlog_red_v"}
    setup_test_file_in_s3(context, tag_map)

    assume_role_for_test(context)


@given(
    "A user is not cleared to access data in the auditlog_sec_v table in the published S3 bucket"
)
@given(
    "A user is cleared to access data in the auditlog_sec_v table in the published S3 bucket"
)
def step_auditlog_sec_v_upload(context):
    context.analytical_test_data_s3_location["path"] = "data/uc/auditlog_sec_v/"

    tag_map = {"pii": "true", "db": "uc", "table": "auditlog_sec_v"}
    setup_test_file_in_s3(context, tag_map)

    assume_role_for_test(context)


@given(
    "A user is cleared to access data in the auditlog_unred_v table in the published S3 bucket"
)
@given(
    "A user is not cleared to access data in the auditlog_unred_v table in the published S3 bucket"
)
def step_auditlog_unred_v_upload(context):
    context.analytical_test_data_s3_location["path"] = "data/uc/auditlog_unred_v/"

    tag_map = {"pii": "true", "db": "uc", "table": "auditlog_unred_v"}
    setup_test_file_in_s3(context, tag_map)

    assume_role_for_test(context)


@given(
    "A user is not cleared to access other tables than uc_ris_unredacted table in the published S3 bucket"
)
@given(
    "A user is only cleared to access ucs_opsmi_redacted table in the published S3 bucket"
)
def step_ucs_opsmi_redacted_upload(context):
    context.analytical_test_data_s3_location["path"] = "data/ucs_opsmi_redacted/"

    tag_map = {"pii": "false", "db": "ucs_opsmi_redacted", "table": "test_table"}
    setup_test_file_in_s3(context, tag_map)

    assume_role_for_test(context)


@given(
    "A user is only cleared to access ucs_opsmi_unredacted table in the published S3 bucket"
)
@given(
    "A user is not cleared to access other tables than ucs_opsmi_redacted table in the published S3 bucket"
)
def step_ucs_opsmi_unredacted_upload(context):
    context.analytical_test_data_s3_location["path"] = "data/ucs_opsmi_unredacted/"

    tag_map = {"pii": "true", "db": "ucs_opsmi_unredacted", "table": "test_table"}
    setup_test_file_in_s3(context, tag_map)

    assume_role_for_test(context)


@given(
    "A user is only cleared to access uc_ris_redacted table in the published S3 bucket"
)
@given(
    "A user is not cleared to access other tables than ucs_opsmi_unredacted table in the published S3 bucket"
)
def step_ucs_opsmi_unredacted_upload(context):
    context.analytical_test_data_s3_location["path"] = "data/uc_ris_redacted/"

    tag_map = {"pii": "false", "db": "uc_ris_redacted", "table": "test_table"}
    setup_test_file_in_s3(context, tag_map)

    assume_role_for_test(context)


@given(
    "A user is only cleared to access uc_ris_unredacted table in the published S3 bucket"
)
@given(
    "A user is not cleared to access other tables than uc_ris_redacted table in the published S3 bucket"
)
def step_ucs_opsmi_unredacted_upload(context):
    context.analytical_test_data_s3_location["path"] = "data/uc_ris_unredacted/"

    tag_map = {"pii": "false", "db": "uc_ris_unredacted", "table": "test_table"}
    setup_test_file_in_s3(context, tag_map)

    assume_role_for_test(context)

@given("A user is cleared to read ucs_latest_redacted DB data in the published S3 bucket")
def step_uc_mongo_latest_assumed(context):
    context.analytical_test_data_s3_location["path"] = "data/ucs_latest_redacted/"

    tag_map = {"pii": "true", "db": "uc_mongo_latest", "table": "test_table"}
    setup_test_file_in_s3(context, tag_map)
    assume_role_for_test(context)

@given("A user is cleared to read uc_mongo_latest DB data in the published S3 bucket")
def step_uc_mongo_latest_assumed(context):
    context.analytical_test_data_s3_location["path"] = "data/uc_mongo_latest/"

    tag_map = {"pii": "true", "db": "uc_mongo_latest", "table": "test_table"}
    setup_test_file_in_s3(context, tag_map)
    assume_role_for_test(context)


@given("A user is only cleared to write to the uc_mongo_latest DB location in the published S3 bucket")
def step_uc_mongo_latest_assumed(context):
    context.analytical_test_data_s3_location["path"] = "data/ucs_latest_redacted/"

    tag_map = {"pii": "false", "db": "ucs_latest_redacted", "table": "test_table"}
    setup_test_file_in_s3(context, tag_map)
    assume_role_for_test(context)


@given("A user is not cleared to write to ucs_latest_redacted DB location in the published S3 bucket")
@given("A user is cleared to write to ucs_latest_redacted DB location in the published S3 bucket")
def step_ucs_latest_redacted_user_write_access(context):
    context.analytical_test_data_s3_location["path"] = "data/ucs_latest_redacted/"
    assume_role_for_test(context)


@given("A user is only cleared to write to the ucs_latest_redacted DB location in the published S3 bucket")
@given("A user is not cleared to write to uc_mongo_latest DB location in the published S3 bucket")
@given("A user is cleared to write to uc_mongo_latest DB location in the published S3 bucket")
def step_uc_mongo_latest_user_write_access(context):
    context.analytical_test_data_s3_location["path"] = "data/uc_mongo_latest/"
    assume_role_for_test(context)


@when('The user attempts to write to another S3 bucket location')
@when('The user attempts to write to the published S3 bucket location')
def step_attempt_to_write_data(context):
    with open(context.analytical_test_data_s3_location["file_name"], 'a'):
        os.utime(context.analytical_test_data_s3_location["file_name"], None)

    context.write_access = aws_helper.test_s3_access_write(
        context.published_bucket,
        os.path.join(
            context.analytical_test_data_s3_location["path"],
            context.analytical_test_data_s3_location["file_name"],
        ),
        context.analytical_test_data_s3_location["file_name"],
        30
    )


@then("The user is able to write to the location")
def step_user_has_write_access(context):
    assert context.write_access == True


@then("The user is unable to write to the location")
def step_user_has_write_access(context):
    assert context.write_access == False


def setup_test_file_in_s3(context, tag_map):
    local_dir = "/tmp/"

    #  Create local file, upload to s3 then delete local file
    file_helper.create_local_file(
        context.analytical_test_data_s3_location["file_name"], local_dir
    )

    aws_helper.upload_file_to_s3_and_wait_for_consistency(
        os.path.join(local_dir, context.analytical_test_data_s3_location["file_name"]),
        context.published_bucket,
        context.timeout,
        os.path.join(
            context.analytical_test_data_s3_location["path"],
            context.analytical_test_data_s3_location["file_name"],
        ),
    )

    file_helper.delete_local_file(
        context.analytical_test_data_s3_location["file_name"], local_dir
    )

    # Tag file uploaded to s3 with 'pii': 'true'
    aws_helper.add_tags_to_file_in_s3(
        context.published_bucket,
        os.path.join(
            context.analytical_test_data_s3_location["path"],
            context.analytical_test_data_s3_location["file_name"],
        ),
        [{"Key": tag, "Value": tag_map[tag]} for tag in tag_map],
    )

def assume_role_for_test(context):
    arn_value = analytical_env_helper.generate_policy_arn(
        context.aws_acc,
        context.analytical_test_e2e_role
    )

    aws_helper.set_details_for_role_assumption(
        arn_value, context.aws_session_timeout_seconds
    )
    aws_helper.clear_session()