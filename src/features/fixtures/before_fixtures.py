import os
import json
import time
from botocore.exceptions import ClientError
from behave import fixture
from helpers import (
    aws_helper,
    template_helper,
    console_printer,
    export_status_helper,
    manifest_comparison_helper,
    snapshots_helper,
    message_helper,
    streaming_manifests_helper,
    streaming_data_helper,
)


@fixture
def s3_clear_dlq(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 's3_clear_dlq' fixture")
    aws_helper.clear_s3_prefix(
        context.s3_ingest_bucket, context.s3_dlq_path_and_date_prefix, True
    )
    context.add_cleanup(print, "Executing 's3_clear_dlq' cleanup")
    context.add_cleanup(
        aws_helper.clear_s3_prefix,
        context.s3_ingest_bucket,
        context.s3_dlq_path_and_date_prefix,
        True,
    )


@fixture
def s3_clear_snapshot_start(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 's3_clear_snapshot_start' fixture")
    aws_helper.clear_s3_prefix(
        context.mongo_snapshot_bucket,
        os.path.join(context.mongo_snapshot_path, context.test_run_name),
        True,
    )
    aws_helper.clear_s3_prefix(
        context.mongo_snapshot_bucket,
        os.path.join(context.snapshot_s3_status_path, context.test_run_name),
        True,
    )


@fixture
def s3_clear_full_snapshot_output(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 's3_clear_full_snapshot_output' fixture")
    s3_clear_snapshot_output(context, "full")


@fixture
def s3_clear_incremental_snapshot_output(context, timeout=30, **kwargs):
    console_printer.print_info(
        "Executing 's3_clear_incremental_snapshot_output' fixture"
    )
    s3_clear_snapshot_output(context, "incremental")


def s3_clear_snapshot_output(context, snapshot_type):
    for topic in context.topics:
        snapshot_s3_full_output_path = (
            snapshots_helper.generate_snapshot_output_s3_path(
                context.snapshot_s3_output_path,
                topic,
                context.db_name,
                context.formatted_date,
                snapshot_type,
            )
        )
        aws_helper.clear_s3_prefix(
            context.snapshot_s3_output_bucket, snapshot_s3_full_output_path, True
        )
        context.add_cleanup(
            print, f"Executing 's3_clear_snapshot_output' cleanup for topic '{topic}'"
        )
        context.add_cleanup(
            aws_helper.clear_s3_prefix,
            context.snapshot_s3_output_bucket,
            snapshot_s3_full_output_path,
            True,
        )


@fixture
def s3_clear_k2hb_manifests_main(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 's3_clear_k2hb_manifests_main' fixture")
    console_printer.print_info(
        f"Clearing manifests from '{context.k2hb_manifest_write_s3_bucket}/{context.k2hb_main_manifest_write_s3_prefix}'"
    )
    aws_helper.clear_s3_prefix(
        context.k2hb_manifest_write_s3_bucket,
        context.k2hb_main_manifest_write_s3_prefix,
        True,
        False,
    )


@fixture
def s3_clear_k2hb_manifests_equalities(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 's3_clear_k2hb_manifests_equalities' fixture")
    console_printer.print_info(
        f"Clearing manifests from '{context.k2hb_manifest_write_s3_bucket}/{context.k2hb_equality_manifest_write_s3_prefix}'"
    )
    aws_helper.clear_s3_prefix(
        context.k2hb_manifest_write_s3_bucket,
        context.k2hb_equality_manifest_write_s3_prefix,
        True,
        False,
    )


@fixture
def s3_clear_k2hb_manifests_audit(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 's3_clear_k2hb_manifests_audit' fixture")
    console_printer.print_info(
        f"Clearing manifests from '{context.k2hb_manifest_write_s3_bucket}/{context.k2hb_audit_manifest_write_s3_prefix}'"
    )
    aws_helper.clear_s3_prefix(
        context.k2hb_manifest_write_s3_bucket,
        context.k2hb_audit_manifest_write_s3_prefix,
        True,
        False,
    )


@fixture
def s3_clear_snapshot(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 's3_clear_snapshot' fixture")
    aws_helper.clear_s3_prefix(
        context.mongo_snapshot_bucket,
        os.path.join(context.mongo_snapshot_path, context.test_run_name),
        True,
    )
    aws_helper.clear_s3_prefix(
        context.mongo_snapshot_bucket,
        os.path.join(context.snapshot_s3_status_path, context.test_run_name),
        True,
    )
    context.add_cleanup(print, "Executing 's3_clear_snapshot' cleanup")
    context.add_cleanup(
        aws_helper.clear_s3_prefix,
        context.mongo_snapshot_bucket,
        os.path.join(context.mongo_snapshot_path, context.test_run_name),
        True,
    )
    context.add_cleanup(
        aws_helper.clear_s3_prefix,
        context.mongo_snapshot_bucket,
        os.path.join(context.snapshot_s3_status_path, context.test_run_name),
        True,
    )


@fixture
def s3_clear_historic_data_start(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 's3_clear_historic_data_start' fixture")
    aws_helper.clear_s3_prefix(
        context.s3_ingest_bucket,
        os.path.join(context.ucfs_historic_data_prefix, context.test_run_name),
        True,
    )


@fixture
def s3_clear_corporate_data_start(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 's3_clear_corporate_data_start' fixture")

    aws_helper.clear_s3_prefix(
        context.corporate_storage_s3_bucket_id,
        context.cdl_data_load_s3_base_prefix_tests,
        True,
    )


@fixture
def s3_clear_pdm_start(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 's3_clear_pdm_start' fixture")
    aws_helper.clear_s3_prefix(
        context.published_bucket,
        os.path.join(context.fixture_path_local, "pdm-test-data"),
        True,
    )


@fixture
def htme_start_full(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'htme_start_full' fixture")
    updated_topics = message_helper.get_consolidated_topics_list(
        context.topics,
        "full",
        context.default_topic_list_full_delimited,
        context.default_topic_list_incremental_delimited,
        [
            context.generate_snapshots_topics_override,
            context.send_snapshots_topics_override,
        ],
    )
    desired_count = manifest_comparison_helper.get_desired_asg_count(
        updated_topics, context.asg_max_count_htme
    )
    context.last_scaled_asg = aws_helper.scale_asg_if_desired_count_is_not_already_set(
        context.asg_prefix_htme, int(desired_count)
    )


@fixture
def htme_start_incremental(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'htme_start_incremental' fixture")
    updated_topics = message_helper.get_consolidated_topics_list(
        context.topics,
        "incremental",
        context.default_topic_list_full_delimited,
        context.default_topic_list_incremental_delimited,
        [
            context.generate_snapshots_topics_override,
            context.send_snapshots_topics_override,
        ],
    )
    desired_count = manifest_comparison_helper.get_desired_asg_count(
        updated_topics, context.asg_max_count_htme
    )
    context.last_scaled_asg = aws_helper.scale_asg_if_desired_count_is_not_already_set(
        context.asg_prefix_htme, int(desired_count)
    )


@fixture
def htme_start_max(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'htme_start_max' fixture")
    context.last_scaled_asg = aws_helper.scale_asg_if_desired_count_is_not_already_set(
        context.asg_prefix_htme, int(context.asg_max_count_htme)
    )


@fixture
def htme_stop(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'htme_stop' fixture")
    context.last_scaled_asg = aws_helper.scale_asg_if_desired_count_is_not_already_set(
        context.asg_prefix_htme, 0
    )


@fixture
def snapshot_sender_start_max(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'snapshot_sender_start_max' fixture")

    desired_count = (
        context.asg_max_count_snapshot_sender
        if not context.snapshot_sender_scale_up_override
        else context.snapshot_sender_scale_up_override
    )

    context.last_scaled_asg = aws_helper.scale_asg_if_desired_count_is_not_already_set(
        context.asg_prefix_snapshot_sender, int(desired_count)
    )


@fixture
def snapshot_sender_stop(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'snapshot_sender_stop' fixture")
    context.last_scaled_asg = aws_helper.scale_asg_if_desired_count_is_not_already_set(
        context.asg_prefix_snapshot_sender, 0
    )


@fixture
def historic_data_importer_start(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'historic_data_importer_start' fixture")
    historic_data_importer_start_base(context, context.test_run_name)


@fixture
def historic_data_importer_start_data_load(context, timeout=30, **kwargs):
    console_printer.print_info(
        "Executing 'historic_data_importer_start_data_load' fixture"
    )
    historic_data_importer_start_base(
        context, context.mongo_data_load_prefixes_comma_delimited
    )


@fixture
def historic_data_importer_start_max(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'historic_data_importer_start_max' fixture")
    historic_data_importer_start_base(
        context, [number for number in range(1, int(context.asg_max_count_hdi) + 1)]
    )


@fixture
def historic_data_importer_start_base(context, prefix_list):
    all_prefixes = template_helper.get_historic_data_importer_prefixes(
        prefix_list, context.historic_importer_use_one_message_per_path
    )

    if len(all_prefixes) == 1:
        all_prefixes.append(
            "This is added because we never want one HDI if max is above 1"
        )

    desired_count = manifest_comparison_helper.get_desired_asg_count(
        all_prefixes, context.asg_max_count_hdi
    )
    context.last_scaled_asg = aws_helper.scale_asg_if_desired_count_is_not_already_set(
        context.asg_prefix_hdi, int(desired_count)
    )


@fixture
def historic_data_importer_stop(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'historic_data_importer_stop' fixture")
    context.last_scaled_asg = aws_helper.scale_asg_if_desired_count_is_not_already_set(
        context.asg_prefix_hdi, 0
    )


@fixture
def hbase_clear_ingest_start(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'hbase_clear_ingest_start' fixture")
    for topic in context.topics:
        aws_helper.truncate_hbase_table(topic)


@fixture
def hbase_clear_ingest_equalities_start(context, timeout=30, **kwargs):
    console_printer.print_info(
        "Executing 'hbase_clear_ingest_equalities_start' fixture"
    )
    for topic in streaming_data_helper.generate_topics_override(
        "kafka_equalities", context.topics
    ):
        aws_helper.truncate_hbase_table(topic["topic"])


@fixture
def hbase_clear_ingest_audit_start(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'hbase_clear_ingest_audit_start' fixture")
    for topic in streaming_data_helper.generate_topics_override(
        "kafka_audit", context.topics
    ):
        aws_helper.truncate_hbase_table(topic["topic"])


@fixture
def hbase_clear_ingest_unique_start(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'hbase_clear_ingest_unique_start' fixture")
    for unique_topic in context.topics_unique:
        aws_helper.truncate_hbase_table(unique_topic)


@fixture
def s3_clear_manifest_output(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 's3_clear_manifest_output' fixture")
    aws_helper.clear_s3_prefix(
        context.manifest_s3_bucket, context.manifest_s3_output_prefix_queries, True
    )
    aws_helper.clear_s3_prefix(
        context.manifest_s3_bucket, context.manifest_s3_output_prefix_templates, True
    )
    aws_helper.clear_s3_prefix(
        context.manifest_s3_bucket, context.manifest_s3_output_parquet_location, False
    )


@fixture
def setup_user(context, primary_policy_name, role_name=None, additional_policies=None):
    context.analytical_test_e2e_policy = primary_policy_name

    if role_name:
        context.analytical_test_e2e_role = role_name
    else:
        context.analytical_test_e2e_role = f"e2e_{primary_policy_name}"

    if additional_policies:
        additional_policies.append(primary_policy_name)
        context.analytical_test_e2e_additional_policies = additional_policies
        policy_list = context.analytical_test_e2e_additional_policies
    else:
        policy_list = [primary_policy_name]

    # Add policy to context to allow for removal in cleanup
    arn_suffix = f"{context.aws_acc}:policy/{policy}"
    arn_value = aws_helper.generate_arn("iam", arn_suffix)
    context.analytical_test_e2e_policies = [arn_value for policy in policy_list]


@fixture
def setup_user_and_role(
    context, primary_policy_name, role_name=None, additional_policies=None
):
    assume_role_document_json = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "sts:AssumeRole", "Principal": {}}],
    }
    arn_suffix = f"{context.aws_acc}:role/ci"
    ci_role_arn = aws_helper.generate_arn("iam", arn_suffix)

    context.analytical_test_e2e_policy = primary_policy_name

    if role_name:
        context.analytical_test_e2e_role = role_name
    else:
        context.analytical_test_e2e_role = f"e2e_{primary_policy_name}"

    # remove role, if exists
    try:
        policy_arns = aws_helper.list_policy_arns_for_role(
            context.analytical_test_e2e_role, context.aws_acc
        )
        aws_helper.remove_role(context.analytical_test_e2e_role, policy_arns)
        console_printer.print_info(
            'Found orphaned "analytical_test_e2e_role" from previous test and removed it.'
        )
    except Exception as e:
        console_printer.print_info(e)
        pass

    assume_role_document_json["Statement"][0]["Principal"].update(
        {"AWS": [ci_role_arn]}
    )

    # Set up role with trust policy
    try:
        role_name = aws_helper.create_role_and_wait_for_it_to_exist(
            context.analytical_test_e2e_role,
            json.dumps(assume_role_document_json),
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            time.sleep(10)
            role_name = aws_helper.create_role_and_wait_for_it_to_exist(
                context.analytical_test_e2e_role,
                json.dumps(assume_role_document_json),
            )
        else:
            raise e

    # Add policy to context to allow for removal in cleanup
    if additional_policies:
        additional_policies.append(primary_policy_name)
        context.analytical_test_e2e_additional_policies = additional_policies
        policy_list = context.analytical_test_e2e_additional_policies
    else:
        policy_list = [primary_policy_name]

    # Add policy to context to allow for removal in cleanup
    arn_suffix = f"{context.aws_acc}:policy/{policy}"
    arn_value = aws_helper.generate_arn("iam", arn_suffix)
    context.analytical_test_e2e_policies = [arn_value for policy in policy_list]

    aws_helper.attach_policies_to_role(
        context.analytical_test_e2e_policies,
        role_name,
    )


@fixture
def analytical_env_setup(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'analytical_env_setup' fixture")
    context.add_cleanup(print, "Executing 'analytical_env_setup' cleanup")
    context.add_cleanup(
        aws_helper.remove_file_from_s3_and_wait_for_consistency,
        context.published_bucket,
        os.path.join(
            context.analytical_test_data_s3_location["path"],
            context.analytical_test_data_s3_location["file_name"],
        ),
    )


@fixture
def kafka_stub_start(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'kafka_stub_start' fixture")
    context.last_scaled_asg = aws_helper.scale_asg_if_desired_count_is_not_already_set(
        context.asg_prefix_kafka_stub, int(context.asg_max_count_kafka_stub)
    )


@fixture
def kafka_stub_stop(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'kafka_stub_stop' fixture")
    context.last_scaled_asg = aws_helper.scale_asg_if_desired_count_is_not_already_set(
        context.asg_prefix_kafka_stub, 0
    )


@fixture
def k2hb_reconciler_start(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'k2hb_reconciler_start' fixture")

    aws_helper.scale_ecs_service_if_desired_count_is_not_already_set(
        context.reconciler_main_cluster_name,
        context.reconciler_main_service_name,
        int(context.reconciler_main_desired_task_count),
    )

    aws_helper.scale_ecs_service_if_desired_count_is_not_already_set(
        context.reconciler_equalities_cluster_name,
        context.reconciler_equalities_service_name,
        int(context.reconciler_equalities_desired_task_count),
    )

    aws_helper.scale_ecs_service_if_desired_count_is_not_already_set(
        context.reconciler_audit_cluster_name,
        context.reconciler_audit_service_name,
        int(context.reconciler_audit_desired_task_count),
    )


@fixture
def k2hb_start(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'k2hb_start' fixture")

    context.last_scaled_asg = aws_helper.scale_asg_if_desired_count_is_not_already_set(
        context.asg_prefix_k2hb_main_london, int(context.asg_max_count_k2hb_main_london)
    )
    context.last_scaled_asg = aws_helper.scale_asg_if_desired_count_is_not_already_set(
        context.asg_prefix_k2hb_main_dedicated_london,
        int(context.asg_max_count_k2hb_main_dedicated_london),
    )
    context.last_scaled_asg = aws_helper.scale_asg_if_desired_count_is_not_already_set(
        context.asg_prefix_k2hb_equality_london,
        int(context.asg_max_count_k2hb_equality_london),
    )
    context.last_scaled_asg = aws_helper.scale_asg_if_desired_count_is_not_already_set(
        context.asg_prefix_k2hb_audit_london,
        int(context.asg_max_count_k2hb_audit_london),
    )


@fixture
def k2hb_stop(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'k2hb_stop' fixture")

    context.last_scaled_asg = aws_helper.scale_asg_if_desired_count_is_not_already_set(
        context.asg_prefix_k2hb_main_london, 0
    )
    context.last_scaled_asg = aws_helper.scale_asg_if_desired_count_is_not_already_set(
        context.asg_prefix_k2hb_main_dedicated_london, 0
    )
    context.last_scaled_asg = aws_helper.scale_asg_if_desired_count_is_not_already_set(
        context.asg_prefix_k2hb_equality_london, 0
    )
    context.last_scaled_asg = aws_helper.scale_asg_if_desired_count_is_not_already_set(
        context.asg_prefix_k2hb_audit_london, 0
    )


@fixture
def ucfs_claimant_kafka_consumer_start(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'ucfs_claimant_kafka_consumer_start' fixture")

    aws_helper.scale_ecs_service_if_desired_count_is_not_already_set(
        context.ucfs_claimant_api_kafka_consumer_cluster_name,
        context.ucfs_claimant_api_kafka_consumer_service_name,
        int(context.ucfs_claimant_api_kafka_consumer_service_desired_task_count),
    )


@fixture
def ucfs_claimant_kafka_consumer_stop(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'ucfs_claimant_kafka_consumer_stop' fixture")

    aws_helper.scale_ecs_service_if_desired_count_is_not_already_set(
        context.ucfs_claimant_api_kafka_consumer_cluster_name,
        context.ucfs_claimant_api_kafka_consumer_service_name,
        0,
    )


@fixture
def dynamodb_clear_ingest_start_full(context, timeout=30, **kwargs):
    console_printer.print_info("Executing 'dynamodb_clear_ingest_start_full' fixture")
    dynamodb_clear_ingest_start(context, "full", context.topics)


@fixture
def dynamodb_clear_ingest_start_incremental(context, timeout=30, **kwargs):
    console_printer.print_info(
        "Executing 'dynamodb_clear_ingest_start_incremental' fixture"
    )
    dynamodb_clear_ingest_start(context, "incremental", context.topics)


@fixture
def dynamodb_clear_ingest_start_unique_full(context, timeout=30, **kwargs):
    console_printer.print_info(
        "Executing 'dynamodb_clear_ingest_start_unique_full' fixture"
    )
    dynamodb_clear_ingest_start(context, "full", context.topics_unique)


@fixture
def dynamodb_clear_ingest_start_unique_incremental(context, timeout=30, **kwargs):
    console_printer.print_info(
        "Executing 'dynamodb_clear_ingest_start_unique_incremental' fixture"
    )
    dynamodb_clear_ingest_start(context, "incremental", context.topics_unique)


@fixture
def dynamodb_clear_ingest_start(context, snapshot_type, topics_list):
    console_printer.print_info("Executing 'dynamodb_clear_ingest_start' fixture")
    updated_topics = message_helper.get_consolidated_topics_list(
        topics_list,
        snapshot_type,
        context.default_topic_list_full_delimited,
        context.default_topic_list_incremental_delimited,
        [
            context.generate_snapshots_topics_override,
            context.send_snapshots_topics_override,
        ],
    )
    correlation_id = (
        snapshots_helper.get_snapshot_run_correlation_id(
            context.test_run_name, snapshot_type
        )
        if not context.send_snapshots_correlation_id_override
        else context.send_snapshots_correlation_id_override
    )

    for topic in updated_topics:
        topic_name = template_helper.get_topic_name(topic)

        export_status_helper.delete_item_in_export_status_table(
            context.dynamo_db_export_status_table_name, topic_name, correlation_id
        )


@fixture
def claimant_api_setup(context):
    console_printer.print_info("Executing 'claimant_api_setup' fixture")
    context.execute_steps(
        f"given The claimant API 'business' region is set to 'Ireland'"
    )
    context.execute_steps(f"given The claimant API 'storage' region is set to 'London'")
    context.execute_steps(f"given The nino salt has been retrieved")


@fixture
def s3_clear_published_bucket_pdm_test_input(context, timeout=30, **kwargs):
    console_printer.print_info(
        "Executing 's3_clear_published_bucket_pdm_test_input' fixture"
    )
    aws_helper.clear_s3_prefix(
        context.published_bucket, context.pdm_test_input_s3_prefix, False
    )


@fixture
def s3_clear_published_bucket_pdm_test_output(context, timeout=30, **kwargs):
    console_printer.print_info(
        "Executing 's3_clear_published_bucket_pdm_test_output' fixture"
    )
    aws_helper.clear_s3_prefix(
        context.published_bucket, context.pdm_test_output_s3_prefix, False
    )
