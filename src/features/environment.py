import os
import re
import uuid

from helpers import (
    aws_helper,
    console_printer,
    file_helper,
    environment_helper,
    monitoring_helper,
)
from fixtures import after_fixtures
from fixtures import before_fixtures
from behave import use_fixture
from behave.model_core import Status


current_feature = None
current_scenario = None
failed_feature = None
failed_scenario = None


def before_all(context):
    # Ensure all required userdata was supplied
    console_printer.set_log_level_info()

    # Set variables that are simply from user data and string manipulations
    environment_helper.set_test_run_common_variables(context)
    environment_helper.set_manifest_variables(context)

    # Assume AWS role before any calls
    aws_helper.set_details_for_role_assumption(
        context.aws_role, context.aws_session_timeout_seconds
    )

    # Generate the topics for this test run
    environment_helper.generate_test_run_topics(context)

    # Clear and delete root temp folder if it exists
    if os.path.exists(context.root_temp_folder):
        console_printer.print_info(
            f"Clearing out root temp folder at '{context.root_temp_folder}'"
        )
        file_helper.clear_and_delete_directory(context.root_temp_folder)

    # Create the root temp folder for test run
    console_printer.print_info(
        f"Creating temp root folder at '{context.root_temp_folder}'"
    )
    os.makedirs(context.root_temp_folder)


def before_feature(context, feature):
    global current_feature

    console_printer.print_info("Executing before feature hook")

    # Set the feature
    current_feature = feature


def before_scenario(context, scenario):
    global current_scenario

    console_printer.print_info("Executing before scenario hook")

    # Create temp scenario folder
    context.temp_folder = os.path.join(context.root_temp_folder, str(uuid.uuid4()))
    console_printer.print_info(
        f"Creating temp scenario folder at '{context.temp_folder}'"
    )
    os.makedirs(context.temp_folder)

    # Reset topics each scenario
    context.topics_for_test = []
    for topic in context.topics:
        context.topics_for_test.append({"topic": topic, "key": str(uuid.uuid4())})

    context.snapshot_files_temp_folder = os.path.join(context.temp_folder, "snapshots")
    os.makedirs(context.snapshot_files_temp_folder)

    context.snapshot_files_hbase_records_temp_folder = os.path.join(
        context.temp_folder, "hbase-records"
    )
    os.makedirs(context.snapshot_files_hbase_records_temp_folder)

    context.historic_data_generations_count_per_test = 0

    # Set the scenario
    current_scenario = scenario


def before_tag(context, tag):
    if tag == "fixture.setup.non_sc_user_and_role":
        use_fixture(before_fixtures.setup_user_and_role, context, "rbac_uc__nonpii")
    if tag == "fixture.setup.non_sc_user":
        use_fixture(before_fixtures.setup_user, context, "rbac_uc__nonpii")
    if tag == "fixture.setup.sc_user_and_role":
        use_fixture(before_fixtures.setup_user_and_role, context, "rbac_uc__pii")
    if tag == "fixture.setup.sc_user":
        use_fixture(before_fixtures.setup_user, context, "rbac_uc__pii")
    if tag == "fixture.setup.auditlog_secure_user_and_role":
        use_fixture(
            before_fixtures.setup_user_and_role, context, "rbac_uc__auditlog_secure"
        )
    if tag == "fixture.setup.auditlog_secure_user":
        use_fixture(before_fixtures.setup_user, context, "rbac_uc__auditlog_secure")
    if tag == "fixture.setup.auditlog_unredacted_user_and_role":
        use_fixture(
            before_fixtures.setup_user_and_role, context, "rbac_uc__auditlog_unredacted"
        )
    if tag == "fixture.setup.auditlog_unredacted_user":
        use_fixture(before_fixtures.setup_user, context, "rbac_uc__auditlog_unredacted")
    if tag == "fixture.setup.auditlog_redacted_user_and_role":
        use_fixture(
            before_fixtures.setup_user_and_role, context, "rbac_uc__auditlog_redacted"
        )
    if tag == "fixture.setup.auditlog_redacted_user":
        use_fixture(before_fixtures.setup_user, context, "rbac_uc__auditlog_redacted")
    if tag == "fixture.setup.rbac_ucs_opsmi_redacted__all_and_role":
        use_fixture(
            before_fixtures.setup_user_and_role, context, "rbac_ucs_opsmi_redacted__all"
        )
    if tag == "fixture.setup.rbac_ucs_opsmi_redacted__all":
        use_fixture(before_fixtures.setup_user, context, "rbac_ucs_opsmi_redacted__all")
    if tag == "fixture.setup.rbac_ucs_opsmi_unredacted__all_and_role":
        use_fixture(
            before_fixtures.setup_user_and_role,
            context,
            "rbac_ucs_opsmi_unredacted__all",
        )
    if tag == "fixture.setup.rbac_ucs_opsmi_unredacted__all":
        use_fixture(
            before_fixtures.setup_user, context, "rbac_ucs_opsmi_unredacted__all"
        )
    if tag == "fixture.setup.rbac_uc_ris_redacted__all_and_role":
        use_fixture(
            before_fixtures.setup_user_and_role, context, "rbac_uc_ris_redacted__all"
        )
    if tag == "fixture.setup.rbac_uc_ris_redacted__all":
        use_fixture(before_fixtures.setup_user, context, "rbac_uc_ris_redacted__all")
    if tag == "fixture.setup.rbac_uc_ris_unredacted__all_and_role":
        use_fixture(
            before_fixtures.setup_user_and_role, context, "rbac_uc_ris_unredacted__all"
        )
    if tag == "fixture.setup.rbac_uc_ris_unredacted__all":
        use_fixture(before_fixtures.setup_user, context, "rbac_uc_ris_unredacted__all")
    if tag == "fixture.setup.rbac_uc__auditlog_redacted_and_sc_and_role":
        use_fixture(
            before_fixtures.setup_user_and_role,
            context,
            "rbac_uc__auditlog_redacted",
            "e2e_rbac_uc__auditlog_redacted_and_sc",
            ["rbac_uc__pii"],
        )
    if tag == "fixture.setup.rbac_uc__auditlog_redacted_and_sc":
        use_fixture(
            before_fixtures.setup_user,
            context,
            "rbac_uc__auditlog_redacted",
            "e2e_rbac_uc__auditlog_redacted_and_sc",
            ["rbac_uc__pii"],
        )
    if tag == "fixture.setup.rbac_uc__auditlog_redacted_and_non_sc_and_role":
        use_fixture(
            before_fixtures.setup_user_and_role,
            context,
            "rbac_uc__auditlog_redacted",
            "e2e_rbac_uc__auditlog_redacted_and_non_sc",
            ["rbac_uc__nonpii"],
        )
    if tag == "fixture.setup.rbac_uc__auditlog_redacted_and_non_sc":
        use_fixture(
            before_fixtures.setup_user,
            context,
            "rbac_uc__auditlog_redacted",
            "e2e_rbac_uc__auditlog_redacted_and_non_sc",
            ["rbac_uc__nonpii"],
        )
    if tag == "fixture.setup.rbac_uc__sc_and_non_sc_and_role":
        use_fixture(
            before_fixtures.setup_user_and_role,
            context,
            "rbac_uc__pii",
            "e2e_rbac_uc__sc_and_non_sc",
            ["rbac_uc__nonpii"],
        )
    if tag == "fixture.setup.rbac_uc__sc_and_non_sc":
        use_fixture(
            before_fixtures.setup_user,
            context,
            "rbac_uc__pii",
            "e2e_rbac_uc__sc_and_non_sc",
            ["rbac_uc__nonpii"],
        )
    if tag == "fixture.setup.rbac_uc__auditlog_redacted_and_auditlog_secure_and_role":
        use_fixture(
            before_fixtures.setup_user_and_role,
            context,
            "rbac_uc__auditlog_redacted",
            "e2e_rbac_uc__auditlog_redacted_and_auditlog_secure",
            ["rbac_uc__auditlog_secure"],
        )
    if tag == "fixture.setup.rbac_uc__auditlog_redacted_and_auditlog_secure":
        use_fixture(
            before_fixtures.setup_user,
            context,
            "rbac_uc__auditlog_redacted",
            "e2e_rbac_uc__auditlog_redacted_and_auditlog_secure",
            ["rbac_uc__auditlog_secure"],
        )
    if (
        tag
        == "fixture.setup.rbac_uc_mongo_latest__uc_mongo_latest__full_access_and_role"
    ):
        use_fixture(
            before_fixtures.setup_user_and_role,
            context,
            "rbac_uc_mongo_latest__full_access",
        )
    if tag == "fixture.setup.rbac_uc_mongo_latest__uc_mongo_latest__full_access":
        use_fixture(
            before_fixtures.setup_user, context, "rbac_uc_mongo_latest__full_access"
        )
    if (
        tag
        == "fixture.setup.rbac_uc_mongo_latest__uc_mongo_latest__read_access_and_role"
    ):
        use_fixture(
            before_fixtures.setup_user_and_role,
            context,
            "rbac_uc_mongo_latest__read_access",
        )
    if tag == "fixture.setup.rbac_uc_mongo_latest__uc_mongo_latest__read_access":
        use_fixture(
            before_fixtures.setup_user, context, "rbac_uc_mongo_latest__read_access"
        )
    if (
        tag
        == "fixture.setup.rbac_ucs_latest_redacted__ucs_latest_redacted__full_access_and_role"
    ):
        use_fixture(
            before_fixtures.setup_user_and_role,
            context,
            "rbac_ucs_latest_redacted__full_access",
        )
    if (
        tag
        == "fixture.setup.rbac_ucs_latest_redacted__ucs_latest_redacted__full_access"
    ):
        use_fixture(
            before_fixtures.setup_user, context, "rbac_ucs_latest_redacted__full_access"
        )
    if (
        tag
        == "fixture.setup.rbac_ucs_latest_redacted__ucs_latest_redacted__read_access_and_role"
    ):
        use_fixture(
            before_fixtures.setup_user_and_role,
            context,
            "rbac_ucs_latest_redacted__read_access",
        )
    if (
        tag
        == "fixture.setup.rbac_ucs_latest_redacted__ucs_latest_redacted__read_access"
    ):
        use_fixture(
            before_fixtures.setup_user, context, "rbac_ucs_latest_redacted__read_access"
        )
    if (
        tag
        == "fixture.setup.rbac_ucs_latest_unredacted__ucs_latest_unredacted__full_access_and_role"
    ):
        use_fixture(
            before_fixtures.setup_user_and_role,
            context,
            "rbac_ucs_latest_unredacted__full_access",
        )
    if (
        tag
        == "fixture.setup.rbac_ucs_latest_unredacted__ucs_latest_unredacted__full_access"
    ):
        use_fixture(
            before_fixtures.setup_user,
            context,
            "rbac_ucs_latest_unredacted__full_access",
        )
    if (
        tag
        == "fixture.setup.rbac_ucs_latest_unredacted__ucs_latest_unredacted__read_access_and_role"
    ):
        use_fixture(
            before_fixtures.setup_user_and_role,
            context,
            "rbac_ucs_latest_unredacted__read_access",
        )
    if (
        tag
        == "fixture.setup.rbac_ucs_latest_unredacted__ucs_latest_unredacted__read_access"
    ):
        use_fixture(
            before_fixtures.setup_user,
            context,
            "rbac_ucs_latest_unredacted__read_access",
        )
    if tag == "fixture.setup.rbac_uc_clive__non_pii_user_and_role":
        use_fixture(
            before_fixtures.setup_user_and_role,
            context,
            "rbac_uc_clive__nonpii",
        )
    if tag == "fixture.setup.rbac_uc_clive__non_pii_user":
        use_fixture(
            before_fixtures.setup_user,
            context,
            "rbac_uc_clive__nonpii",
        )
    if tag == "fixture.setup.rbac_uc_clive__pii_user_and_role":
        use_fixture(
            before_fixtures.setup_user_and_role,
            context,
            "rbac_uc_clive__pii",
        )
    if tag == "fixture.setup.rbac_uc_clive__pii_user":
        use_fixture(
            before_fixtures.setup_user,
            context,
            "rbac_uc_clive__pii",
        )
    if tag == "fixture.htme.start.full":
        use_fixture(before_fixtures.htme_start_full, context)
    if tag == "fixture.htme.start.incremental":
        use_fixture(before_fixtures.htme_start_incremental, context)
    if tag == "fixture.htme.start.max":
        use_fixture(before_fixtures.htme_start_max, context)
    if tag == "fixture.snapshot.sender.start.max":
        use_fixture(before_fixtures.snapshot_sender_start_max, context)
    if tag == "fixture.historic.data.importer.start":
        use_fixture(before_fixtures.historic_data_importer_start, context)
    if tag == "fixture.historic.data.importer.start.data.load":
        use_fixture(before_fixtures.historic_data_importer_start_data_load, context)
    if tag == "fixture.historic.data.importer.start.max":
        use_fixture(before_fixtures.historic_data_importer_start_max, context)
    if tag == "fixture.s3.clear.dlq":
        use_fixture(before_fixtures.s3_clear_dlq, context)
    if tag == "fixture.s3.clear.snapshot":
        use_fixture(before_fixtures.s3_clear_snapshot, context)
    if tag == "fixture.s3.clear.snapshot.start":
        use_fixture(before_fixtures.s3_clear_snapshot_start, context)
    if tag == "fixture.s3.clear.hive.query.output.start":
        use_fixture(before_fixtures.s3_clear_hive_query_output_start, context)
    if tag == "fixture.s3.clear.k2hb.manifests.main.start":
        use_fixture(before_fixtures.s3_clear_k2hb_manifests_main, context)
    if tag == "fixture.s3.clear.k2hb.manifests.equalities.start":
        use_fixture(before_fixtures.s3_clear_k2hb_manifests_equalities, context)
    if tag == "fixture.s3.clear.k2hb.manifests.audit.start":
        use_fixture(before_fixtures.s3_clear_k2hb_manifests_audit, context)
    if tag == "fixture.s3.clear.full.snapshot.output":
        use_fixture(before_fixtures.s3_clear_full_snapshot_output, context)
    if tag == "fixture.s3.clear.incremental.snapshot.output":
        use_fixture(before_fixtures.s3_clear_incremental_snapshot_output, context)
    if tag == "fixture.s3.clear.historic.data":
        use_fixture(before_fixtures.s3_clear_historic_data_start, context)
    if tag == "fixture.s3.clear.corporate.data.start":
        use_fixture(before_fixtures.s3_clear_corporate_data_start, context)
    if tag == "fixture.hbase.clear.ingest.start":
        use_fixture(before_fixtures.hbase_clear_ingest_start, context)
    if tag == "fixture.hbase.clear.ingest.equalities.start":
        use_fixture(before_fixtures.hbase_clear_ingest_equalities_start, context)
    if tag == "fixture.hbase.clear.ingest.audit.start":
        use_fixture(before_fixtures.hbase_clear_ingest_audit_start, context)
    if tag == "fixture.hbase.clear.ingest.unique.start":
        use_fixture(before_fixtures.hbase_clear_ingest_unique_start, context)
    if tag == "fixture.s3.clear.manifest":
        use_fixture(before_fixtures.s3_clear_manifest_output, context)
    if tag == "fixture.hdi.stop":
        use_fixture(before_fixtures.historic_data_importer_stop, context)
    if tag == "fixture.htme.stop":
        use_fixture(before_fixtures.htme_stop, context)
    if tag == "fixture.snapshot.sender.stop":
        use_fixture(before_fixtures.snapshot_sender_stop, context)
    if tag == "fixture.kafka.stub.start":
        use_fixture(before_fixtures.kafka_stub_start, context)
    if tag == "fixture.kafka.stub.stop":
        use_fixture(before_fixtures.kafka_stub_stop, context)
    if tag == "fixture.k2hb.reconciler.start":
        use_fixture(before_fixtures.k2hb_reconciler_start, context)
    if tag == "fixture.k2hb.start":
        use_fixture(before_fixtures.k2hb_start, context)
    if tag == "fixture.k2hb.stop":
        use_fixture(before_fixtures.k2hb_stop, context)
    if tag == "fixture.ingest.ecs.cluster.start":
        use_fixture(before_fixtures.ingest_ecs_cluster_start, context)
    if tag == "fixture.ingest.ecs.cluster.stop":
        use_fixture(before_fixtures.ingest_ecs_cluster_stop, context)
    if tag == "fixture.analytical.setup":
        use_fixture(before_fixtures.analytical_env_setup, context)
    if tag == "fixture.dynamodb.clear.ingest.start.full":
        use_fixture(before_fixtures.dynamodb_clear_ingest_start_full, context)
    if tag == "fixture.dynamodb.clear.ingest.start.incremental":
        use_fixture(before_fixtures.dynamodb_clear_ingest_start_incremental, context)
    if tag == "fixture.dynamodb.clear.ingest.start.unique.full":
        use_fixture(before_fixtures.dynamodb_clear_ingest_start_unique_full, context)
    if tag == "fixture.dynamodb.clear.ingest.start.unique.incremental":
        use_fixture(
            before_fixtures.dynamodb_clear_ingest_start_unique_incremental, context
        )
    if tag == "fixture.ucfs.claimant.kafka.consumer.start":
        use_fixture(before_fixtures.ucfs_claimant_kafka_consumer_start, context)
    if tag == "fixture.ucfs.claimant.kafka.consumer.stop":
        use_fixture(before_fixtures.ucfs_claimant_kafka_consumer_stop, context)
    if tag == "fixture.claimant.api.setup":
        use_fixture(before_fixtures.claimant_api_setup, context)
    if tag == "fixture.s3.clear.published.bucket.pdm.test.input":
        use_fixture(before_fixtures.s3_clear_published_bucket_pdm_test_input, context)
    if tag == "fixture.s3.clear.published.bucket.pdm.test.output":
        use_fixture(before_fixtures.s3_clear_published_bucket_pdm_test_output, context)
    if tag == "fixture.s3.clear.clive.output":
        use_fixture(before_fixtures.s3_clear_clive_output, context)
    if tag == "fixture.s3.clear.kickstart.start":
        use_fixture(before_fixtures.s3_clear_kickstart_start, context)


def after_all(context):
    global current_feature
    global current_scenario
    global failed_feature
    global failed_scenario

    # Send notification
    if not context.suppress_monitoring and (context.failed or context.aborted):
        console_printer.print_info("Sending out notification")

        custom_elements = []
        is_test = current_feature is not None and "test" in current_feature.tags

        if failed_feature is not None:
            custom_elements.append(("Feature", failed_feature.name))
            custom_elements.append(
                ("Is hook failure?", str(failed_feature.hook_failed))
            )

        if failed_scenario is not None:
            custom_elements.append(("Scenario", failed_scenario.name))

        if "test_run_name" in context and context.test_run_name is not None:
            custom_elements.append(("Run name", context.test_run_name))

        if context.aborted:
            status = "Test run aborted" if is_test else "Automated job aborted"
            severity = "Medium"
            notification_type = "Warning"
        else:
            status = "Test run failed" if is_test else "Automated job failed"
            severity = "High"
            notification_type = "Error"

        monitoring_helper.send_monitoring_alert(
            context.monitoring_sns_topic_arn,
            status,
            severity,
            notification_type,
            custom_elements,
        )

    console_printer.print_info(
        ""
    )  # This is used as behave always swallows the last console line


def after_feature(context, feature):
    global failed_feature

    console_printer.print_info("Executing after feature hook")

    # Set the feature
    if feature.status == Status.failed:
        failed_feature = feature


def after_scenario(context, scenario):
    global failed_scenario

    console_printer.print_info("Executing after scenario hook")

    # Clear and delete scenario temp folder after scenario
    console_printer.print_info(
        f"Clearing out scenario temp folder at '{context.temp_folder}'"
    )
    file_helper.clear_and_delete_directory(context.temp_folder)

    aws_helper.clear_session()
    aws_helper.set_details_for_role_assumption(
        context.aws_role, context.aws_session_timeout_seconds
    )

    # Set the scenario
    if scenario.status == Status.failed:
        failed_scenario = scenario


def after_tag(context, tag):
    if tag == "fixture.cleanup.role_and_s3":
        use_fixture(after_fixtures.clean_up_role_and_s3_objects, context)
    if tag == "fixture.cleanup.s3":
        use_fixture(after_fixtures.clean_up_s3_object, context)
    if tag == "fixture.terminate.adg.cluster":
        use_fixture(after_fixtures.terminate_adg_cluster, context)
    if tag == "fixture.terminate.clive.cluster":
        use_fixture(after_fixtures.terminate_clive_cluster, context)
    if tag == "fixture.terminate.pdm.cluster":
        use_fixture(after_fixtures.terminate_pdm_cluster, context)
    if tag == "fixture.terminate.kickstart.cluster":
        use_fixture(after_fixtures.terminate_kickstart_cluster, context)
    if tag == "fixture.terminate.mongo_latest.cluster":
        use_fixture(after_fixtures.terminate_mongo_latest_cluster, context)
