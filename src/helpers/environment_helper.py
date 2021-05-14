import os
import datetime
import re

from helpers import (
    template_helper,
    date_helper,
    manifest_comparison_helper,
    data_load_helper,
    console_printer,
)


def set_test_run_common_variables(context):
    """All variables common to all test runs.

    Keyword arguments:
    context -- the behave context object
    """
    console_printer.print_info(f"Setting test run variables common to all test runs")

    context.test_run_name = (
        context.config.userdata.get("TEST_RUN_NAME").replace("-", "_").replace(".", "_")
    )

    context.suppress_monitoring = (
        context.config.userdata.get("SUPPRESS_MONITORING_ALERTS") == "true"
    )

    context.e2e_tag_filter = context.config.userdata.get("E2E_FEATURE_TAG_FILTER")

    context.s3_ingest_bucket = context.config.userdata.get("AWS_S3_INPUT_BUCKET")
    context.mongo_snapshot_bucket = context.config.userdata.get("MONGO_SNAPSHOT_BUCKET")
    context.mongo_snapshot_path = context.config.userdata.get("MONGO_SNAPSHOT_PATH")
    context.mongo_latest_input_s3_prefix = os.path.join(
        "analytical-dataset", "mongo_latest"
    )
    context.mongo_latest_test_query_output_folder = os.path.join(
        context.mongo_snapshot_path, context.test_run_name, "output"
    )
    context.mongo_data_load_prefixes_comma_delimited = context.config.userdata.get(
        "MONGO_DATA_LOAD_PREFIXES"
    )
    context.fixture_path_remote = context.config.userdata.get("FIXTURE_PATH_REMOTE")
    context.ucfs_historic_data_prefix = context.config.userdata.get(
        "UCFS_HISTORIC_DATA_PREFIX"
    )

    context.load_test_key_method = context.config.userdata.get("LOAD_TEST_KEY_METHOD")
    context.number_of_topics_to_use = context.config.userdata.get(
        "NUMBER_OF_TOPICS_TO_USE"
    )
    context.aws_sns_uc_ecc_arn = context.config.userdata.get("AWS_SNS_UC_ECC_ARN")
    context.aws_sqs_queue_historic_data_importer = context.config.userdata.get(
        "AWS_SQS_HISTORIC_DATA_IMPORTER_QUEUE_URL"
    )
    context.aws_sqs_queue_snapshot_sender = context.config.userdata.get(
        "AWS_SQS_SNAPSHOT_SENDER_QUEUE_URL"
    )
    context.aws_acc = context.config.userdata.get("AWS_ACC")
    context.aws_role = context.config.userdata.get("AWS_ROLE_ARN")
    context.aws_session_timeout_seconds = context.config.userdata.get(
        "AWS_SESSION_TIMEOUT_SECONDS"
    )
    context.aws_sns_topic_name = context.config.userdata.get("AWS_SNS_TOPIC_NAME")
    context.dynamo_db_table_name = context.config.userdata.get("DYNAMODB_TABLE_NAME")
    context.timeout = int(context.config.userdata.get("E2E_TEST_TIMEOUT"))
    context.s3_dlq_path_prefix = context.config.userdata.get("DLQ_S3_PATH_PREFIX")
    context.encryption_plaintext_key = context.config.userdata.get(
        "PLAINTEXT_ENCRYPTION_KEY"
    )
    context.encryption_encrypted_key = context.config.userdata.get(
        "ENCRYPTED_ENCRYPTION_KEY"
    )
    context.encryption_master_key_id = context.config.userdata.get("MASTER_KEY_ID")

    context.load_tests_file_count = context.config.userdata.get("LOAD_TESTS_FILE_COUNT")
    context.load_tests_record_count = context.config.userdata.get(
        "LOAD_TESTS_RECORD_COUNT"
    )
    context.load_tests_data_max_worker_count = context.config.userdata.get(
        "LOAD_TESTS_DATA_MAX_WORKER_COUNT"
    )

    context.aws_datasets_bucket = context.config.userdata.get("AWS_DATASETS_BUCKET")
    context.synthetic_rawdata_aws_acc = context.config.userdata.get(
        "SYNTHETIC_RAWDATA_AWS_ACC"
    )
    context.synthetic_rawdata_prefix = context.config.userdata.get(
        "SYNTHETIC_RAWDATA_PREFIX"
    )
    context.synthetic_encdata_prefix = context.config.userdata.get(
        "SYNTHETIC_ENCDATA_PREFIX"
    )
    context.aws_default_profile = context.config.userdata.get("AWS_DEFAULT_PROFILE")
    context.db_name = "automatedtests"

    context.fixture_path_local = "fixture-data/functional-tests"
    context.formatted_date = str(datetime.date.today())
    context.root_temp_folder = "tmp"

    context.s3_dlq_path_and_date_prefix = os.path.join(
        context.s3_dlq_path_prefix,
        template_helper.get_dlq_topic_name(),
        context.formatted_date,
    )
    context.s3_temp_output_path = os.path.join(
        context.fixture_path_remote, "test-output"
    )
    context.s3_prefix_historic_data_input = (
        f"{context.ucfs_historic_data_prefix}/{context.test_run_name}"
    )

    context.k2hb_manifest_write_s3_bucket = context.config.userdata.get(
        "K2HB_MANIFEST_WRITE_S3_BUCKET"
    )
    context.k2hb_main_manifest_write_s3_prefix = context.config.userdata.get(
        "K2HB_MAIN_MANIFEST_WRITE_S3_PREFIX"
    )
    context.k2hb_equality_manifest_write_s3_prefix = context.config.userdata.get(
        "K2HB_EQUALITY_MANIFEST_WRITE_S3_PREFIX"
    )
    context.k2hb_audit_manifest_write_s3_prefix = context.config.userdata.get(
        "K2HB_AUDIT_MANIFEST_WRITE_S3_PREFIX"
    )

    context.generate_snapshots_topics_override = context.config.userdata.get(
        "GENERATE_SNAPSHOTS_TOPICS_OVERRIDE"
    )
    context.send_snapshots_topics_override = context.config.userdata.get(
        "SEND_SNAPSHOTS_TOPICS_OVERRIDE"
    )
    context.send_snapshots_correlation_id_override = context.config.userdata.get(
        "SEND_SNAPSHOTS_CORRELATION_ID_OVERRIDE"
    )
    context.generate_snapshots_correlation_id_override = context.config.userdata.get(
        "GENERATE_SNAPSHOTS_CORRELATION_ID_OVERRIDE"
    )
    context.send_snapshots_date_override = context.config.userdata.get(
        "SEND_SNAPSHOTS_DATE_OVERRIDE"
    )
    context.generate_snapshots_export_date_override = context.config.userdata.get(
        "GENERATE_SNAPSHOTS_EXPORT_DATE_OVERRIDE"
    )

    send_snapshots_reprocess_filescontext_string = context.config.userdata.get(
        "SEND_SNAPSHOTS_REPROCESS_FILES_OVERRIDE"
    )
    context.send_snapshots_reprocess_files = (
        send_snapshots_reprocess_filescontext_string == "true"
    )

    context.asg_max_count_htme = context.config.userdata.get("ASG_MAX_COUNT_HTME")
    context.asg_max_count_hdi = context.config.userdata.get("ASG_MAX_COUNT_HDI")
    context.asg_max_count_snapshot_sender = context.config.userdata.get(
        "ASG_MAX_COUNT_SNAPSHOT_SENDER"
    )
    context.asg_max_count_kafka_stub = context.config.userdata.get(
        "ASG_MAX_COUNT_KAFKA_STUB"
    )
    context.asg_max_count_k2hb_main_london = context.config.userdata.get(
        "ASG_MAX_COUNT_K2HB_MAIN_LONDON"
    )
    context.asg_max_count_k2hb_main_dedicated_london = context.config.userdata.get(
        "ASG_MAX_COUNT_K2HB_MAIN_DEDICATED_LONDON"
    )
    context.asg_max_count_k2hb_equality_london = context.config.userdata.get(
        "ASG_MAX_COUNT_K2HB_EQUALITY_LONDON"
    )
    context.asg_max_count_k2hb_audit_london = context.config.userdata.get(
        "ASG_MAX_COUNT_K2HB_AUDIT_LONDON"
    )
    context.asg_prefix_ingestion_ecs_cluster = context.config.userdata.get(
        "ASG_PREFIX_INGESTION_ECS_CLUSTER"
    )
    context.asg_max_count_ingestion_ecs_cluster = context.config.userdata.get(
        "ASG_MAX_COUNT_INGESTION_ECS_CLUSTER"
    )

    context.asg_prefix_htme = context.config.userdata.get("ASG_PREFIX_HTME")
    context.asg_prefix_hdi = context.config.userdata.get("ASG_PREFIX_HDI")
    context.asg_prefix_snapshot_sender = context.config.userdata.get(
        "ASG_PREFIX_SNAPSHOT_SENDER"
    )
    context.asg_prefix_kafka_stub = context.config.userdata.get("ASG_PREFIX_KAFKA_STUB")

    context.asg_prefix_k2hb_main_london = context.config.userdata.get(
        "ASG_PREFIX_K2HB_MAIN_LONDON"
    )
    context.asg_prefix_k2hb_main_dedicated_london = context.config.userdata.get(
        "ASG_PREFIX_K2HB_MAIN_DEDICATED_LONDON"
    )
    context.asg_prefix_k2hb_equality_london = context.config.userdata.get(
        "ASG_PREFIX_K2HB_EQUALITY_LONDON"
    )
    context.asg_prefix_k2hb_audit_london = context.config.userdata.get(
        "ASG_PREFIX_K2HB_AUDIT_LONDON"
    )

    context.generate_snapshots_start_time_override = context.config.userdata.get(
        "GENERATE_SNAPSHOTS_START_TIME_OVERRIDE"
    )
    context.generate_snapshots_end_time_override = context.config.userdata.get(
        "GENERATE_SNAPSHOTS_END_TIME_OVERRIDE"
    )
    generate_snapshots_reprocess_filescontext_string = context.config.userdata.get(
        "GENERATE_SNAPSHOTS_REPROCESS_FILES_OVERRIDE"
    )
    context.generate_snapshots_reprocess_files = (
        generate_snapshots_reprocess_filescontext_string == "true"
    )
    context.dynamo_db_export_status_table_name = context.config.userdata.get(
        "DYNAMO_DB_EXPORT_STATUS_TABLE_NAME"
    )

    context.kafka_message_volume = context.config.userdata.get("KAFKA_MESSAGE_VOLUME")
    context.kafka_random_key = context.config.userdata.get("KAFKA_RANDOM_KEY")
    context.kafka_record_count = context.config.userdata.get("KAFKA_RECORD_COUNT")

    default_topic_list_full = context.config.userdata.get(
        "HTME_DEFAULT_TOPIC_LIST_FULL_CONTENT"
    ).split("\n")

    default_topic_list_incrementals = context.config.userdata.get(
        "HTME_DEFAULT_TOPIC_LIST_INCREMENTALS_CONTENT"
    ).split("\n")

    default_topic_list_full_delimited_with_space = "', '".join(default_topic_list_full)
    collection_list_full = (
        "'" + default_topic_list_full_delimited_with_space.replace("db.", "") + "'"
    )
    context.distinct_default_database_collection_list_full = (
        collection_list_full.replace(", ''", "")
    )
    context.distinct_default_database_list_full = re.compile(r"\.[\w-]+").sub(
        "", context.distinct_default_database_collection_list_full
    )
    context.default_topic_list_full_delimited = ",".join(default_topic_list_full)

    context.default_topic_list_incremental_delimited = ",".join(
        default_topic_list_incrementals
    )

    context.export_process_trigger_adg_override = context.config.userdata.get(
        "GENERATE_SNAPSHOTS_TRIGGER_ADG_OVERRIDE"
    )
    context.export_process_trigger_pdm_override = context.config.userdata.get(
        "GENERATE_SNAPSHOTS_TRIGGER_PDM_OVERRIDE"
    )

    context.generate_snapshots_trigger_snapshot_sender_override = (
        context.config.userdata.get(
            "GENERATE_SNAPSHOTS_TRIGGER_SNAPSHOT_SENDER_OVERRIDE"
        )
    )

    context.snapshot_s3_output_path = context.config.userdata.get(
        "SNAPSHOT_S3_OUTPUT_PATH"
    )
    context.snapshot_s3_output_bucket = context.config.userdata.get(
        "SNAPSHOT_S3_OUTPUT_BUCKET"
    )
    context.snapshot_s3_status_path = context.config.userdata.get(
        "SNAPSHOT_S3_STATUS_PATH"
    )

    historic_importer_use_one_message_per_path_string = context.config.userdata.get(
        "HISTORIC_IMPORTER_USE_ONE_MESSAGE_PER_PATH"
    )
    context.historic_importer_use_one_message_per_path = (
        historic_importer_use_one_message_per_path_string == "true"
    )
    context.historic_data_ingestion_skip_earlier_than_override = (
        context.config.userdata.get(
            "HISTORIC_DATA_INGESTION_SKIP_EARLIER_THAN_OVERRIDE"
        )
    )
    context.historic_data_ingestion_skip_later_than_override = (
        context.config.userdata.get("HISTORIC_DATA_INGESTION_SKIP_LATER_THAN_OVERRIDE")
    )
    context.historic_data_ingestion_skip_existing_records_override = (
        context.config.userdata.get(
            "HISTORIC_DATA_INGESTION_SKIP_EXISTING_RECORDS_OVERRIDE"
        )
    )
    context.corporate_data_ingestion_skip_earlier_than_override = (
        context.config.userdata.get(
            "CORPORATE_DATA_INGESTION_SKIP_EARLIER_THAN_OVERRIDE"
        )
    )
    context.corporate_data_prefix_per_execution_count_override = (
        context.config.userdata.get(
            "CORPORATE_DATA_INGESTION_PREFIX_PER_EXECUTION_OVERRIDE"
        )
    )

    context.corporate_data_ingestion_partitions_count_override = (
        context.config.userdata.get(
            "CORPORATE_DATA_INGESTION_PARTITIONS_COUNT_OVERRIDE"
        )
    )
    context.corporate_data_ingestion_skip_later_than_override = (
        context.config.userdata.get("CORPORATE_DATA_INGESTION_SKIP_LATER_THAN_OVERRIDE")
    )
    context.historic_importer_correlation_id_override = context.config.userdata.get(
        "HISTORIC_IMPORTER_CORRELATION_ID_OVERRIDE"
    )
    context.snapshot_sender_scale_up_override = context.config.userdata.get(
        "SNAPSHOT_SENDER_SCALE_UP_OVERRIDE"
    )
    context.published_bucket = context.config.userdata.get("AWS_PUBLISHED_BUCKET")

    context.ingest_hbase_emr_cluster_id = context.config.userdata.get(
        "INGEST_HBASE_EMR_CLUSTER_ID"
    )
    context.ingest_hbase_emr_cluster_root_s3_bucket_id = context.config.userdata.get(
        "INGEST_HBASE_EMR_CLUSTER_ROOT_S3_BUCKET_ID"
    )
    context.ingest_hbase_emr_cluster_root_s3_root_directory = (
        context.config.userdata.get("INGEST_HBASE_EMR_CLUSTER_ROOT_S3_ROOT_DIRECTORY")
    )
    context.ingest_hbase_snapshot_tables_override = context.config.userdata.get(
        "INGEST_HBASE_EMR_CLUSTER_SNAPSHOT_TABLES_OVERRIDE"
    )
    context.ingest_hbase_hbck_arguments = context.config.userdata.get(
        "INGEST_HBASE_EMR_CLUSTER_HBCK_ARGUMENTS"
    )
    context.ingest_hbase_emrfs_arguments = context.config.userdata.get(
        "INGEST_HBASE_EMR_CLUSTER_EMRFS_ARGUMENTS"
    )
    context.ingest_hbase_emrfs_prefix_override = context.config.userdata.get(
        "INGEST_HBASE_EMR_CLUSTER_EMRFS_PREFIX_OVERRIDE"
    )

    context.data_streaming_tests_skip_reconciling = (
        context.config.userdata.get("DATA_STREAMING_TESTS_SKIP_RECONCILING") == "true"
    )
    context.metadata_store_table_name_ucfs = context.config.userdata.get(
        "METADATA_STORE_TABLE_NAME_UCFS"
    )
    context.metadata_store_table_name_equalities = context.config.userdata.get(
        "METADATA_STORE_TABLE_NAME_EQUALITIES"
    )
    context.metadata_store_table_name_audit = context.config.userdata.get(
        "METADATA_STORE_TABLE_NAME_AUDIT"
    )
    context.reconciler_main_cluster_name = context.config.userdata.get(
        "RECONCILER_MAIN_CLUSTER_NAME"
    )
    context.reconciler_equalities_cluster_name = context.config.userdata.get(
        "RECONCILER_EQUALITIES_CLUSTER_NAME"
    )
    context.reconciler_audit_cluster_name = context.config.userdata.get(
        "RECONCILER_AUDIT_CLUSTER_NAME"
    )
    context.reconciler_main_service_name = context.config.userdata.get(
        "RECONCILER_MAIN_SERVICE_NAME"
    )
    context.reconciler_equalities_service_name = context.config.userdata.get(
        "RECONCILER_EQUALITIES_SERVICE_NAME"
    )
    context.reconciler_audit_service_name = context.config.userdata.get(
        "RECONCILER_AUDIT_SERVICE_NAME"
    )
    context.reconciler_main_desired_task_count = int(
        context.config.userdata.get("RECONCILER_MAIN_DESIRED_TASK_COUNT")
    )
    context.reconciler_equalities_desired_task_count = int(
        context.config.userdata.get("RECONCILER_EQUALITIES_DESIRED_TASK_COUNT")
    )
    context.reconciler_audit_desired_task_count = int(
        context.config.userdata.get("RECONCILER_AUDIT_DESIRED_TASK_COUNT")
    )

    context.metadata_store_tables = [
        ["main", context.metadata_store_table_name_ucfs],
        ["equalities", context.metadata_store_table_name_equalities],
        ["audit", context.metadata_store_table_name_audit],
    ]

    context.cdl_run_script_s3_url = context.config.userdata.get("CDL_RUN_SCRIPT_S3_URL")
    context.hdl_run_script_s3_url = context.config.userdata.get("HDL_RUN_SCRIPT_S3_URL")
    context.create_hbase_tables_script_url = context.config.userdata.get(
        "CREATE_HBASE_TABLES_SCRIPT_S3_URL"
    )
    context.data_load_topics = context.config.userdata.get("DATA_LOAD_TOPICS")
    context.data_load_s3_suffix = context.config.userdata.get("DATA_LOAD_S3_SUFFIX")
    context.data_load_metadata_store_table = context.config.userdata.get(
        "DATA_LOAD_METADATA_STORE_TABLE"
    )
    context.historic_data_load_run_script_arguments = (
        data_load_helper.generate_arguments_for_historic_data_load(
            context.test_run_name,
            context.data_load_topics,
            context.ucfs_historic_data_prefix,
            context.data_load_s3_suffix,
            context.default_topic_list_full_delimited,
            context.historic_data_ingestion_skip_earlier_than_override,
            context.historic_data_ingestion_skip_later_than_override,
        )
    )
    context.corporate_data_load_run_script_arguments = (
        data_load_helper.generate_arguments_for_corporate_data_load(
            context.test_run_name,
            context.data_load_topics,
            context.config.userdata.get("DATA_LOAD_S3_BASE_PREFIX"),
            context.data_load_metadata_store_table,
            context.default_topic_list_full_delimited,
            context.config.userdata.get("DATA_LOAD_S3_FILE_PATTERN"),
            context.corporate_data_ingestion_skip_earlier_than_override,
            context.corporate_data_ingestion_skip_later_than_override,
            context.corporate_data_ingestion_partitions_count_override,
            context.corporate_data_prefix_per_execution_count_override,
        )
    )

    context.cdl_data_load_s3_base_prefix_tests = os.path.join(
        context.config.userdata.get("CDL_DATA_LOAD_S3_BASE_PREFIX"),
        "automatedtests",
        context.test_run_name,
    )

    context.cdl_file_pattern_ucfs = context.config.userdata.get(
        "CDL_DATA_LOAD_FILE_PATTERN_UCFS"
    )

    context.corporate_storage_s3_bucket_id = context.config.userdata.get(
        "CORPORATE_STORAGE_S3_BUCKET_ID"
    )

    context.ucfs_claimant_domain_name = context.config.userdata.get(
        "UCFS_CLAIMANT_DOMAIN_NAME"
    )
    context.ucfs_claimant_api_path_v1_get_award_details = context.config.userdata.get(
        "UCFS_CLAIMANT_API_PATH_V1_GET_AWARD_DETAILS"
    )
    context.ucfs_claimant_api_path_v2_get_award_details = context.config.userdata.get(
        "UCFS_CLAIMANT_API_PATH_V2_GET_AWARD_DETAILS"
    )
    context.ucfs_claimant_api_salt_ssm_parameter_name = context.config.userdata.get(
        "UCFS_CLAIMANT_API_SALT_SSM_PARAMETER_NAME"
    )
    context.ucfs_claimant_api_kafka_consumer_cluster_name = context.config.userdata.get(
        "UCFS_CLAIMANT_API_KAFKA_CONSUMER_CLUSTER_NAME"
    )
    context.ucfs_claimant_api_kafka_consumer_service_name = context.config.userdata.get(
        "UCFS_CLAIMANT_API_KAFKA_CONSUMER_SERVICE_NAME"
    )
    context.ucfs_claimant_api_kafka_consumer_service_desired_task_count = (
        context.config.userdata.get(
            "UCFS_CLAIMANT_API_KAFKA_CONSUMER_SERVICE_DESIRED_TASK_COUNT"
        )
    )

    context.monitoring_sns_topic_arn = context.config.userdata.get(
        "MONITORING_SNS_TOPIC_ARN"
    )

    context.analytical_test_data_s3_location = {
        "path": "e2e_test_dir/",
        "file_name": "e2e_test_file.txt",
    }

    context.pdm_test_input_s3_prefix = "e2e-test-pdm-dataset"

    context.pdm_test_output_s3_prefix = "e2e-test-pdm-output"

    context.clive_test_input_s3_prefix = "e2e-test-clive-dataset"

    context.clive_output_s3_prefix = "data/uc_clive"

    context.aws_region_main = context.config.userdata.get("AWS_REGION_MAIN")
    context.aws_region_alternative = context.config.userdata.get(
        "AWS_REGION_ALTERNATIVE"
    )


def set_manifest_variables(context):
    """All variables common to manifest test runs.

    Keyword arguments:
    context -- the behave context object
    """
    console_printer.print_info(
        f"Setting test run variables common to manifest test runs"
    )

    context.manifest_templates_local = "manifest-comparison/templates"
    context.manifest_queries_local = "manifest-comparison/queries"
    context.manifest_query_tables_local = "manifest-comparison/query_tables"
    context.manifest_s3_bucket = context.config.userdata.get("MANIFEST_S3_BUCKET")
    context.manifest_import_type = context.config.userdata.get(
        "MANIFEST_COMPARISON_IMPORT_TYPE"
    )
    context.manifest_snapshot_type = context.config.userdata.get(
        "MANIFEST_COMPARISON_SNAPSHOT_TYPE"
    )
    manifest_comparison_helper.validate_manifest_sources(
        context.manifest_import_type, context.manifest_snapshot_type
    )

    if context.manifest_import_type.lower() == "streaming_main":
        context.manifest_s3_input_location_import_prefix = context.config.userdata.get(
            "MANIFEST_S3_INPUT_LOCATION_IMPORT_STREAMING_MAIN"
        )
    elif context.manifest_import_type.lower() == "streaming_equality":
        context.manifest_s3_input_location_import_prefix = context.config.userdata.get(
            "MANIFEST_S3_INPUT_LOCATION_IMPORT_STREAMING_EQUALITY"
        )
    else:
        context.manifest_s3_input_location_import_prefix = context.config.userdata.get(
            "MANIFEST_S3_INPUT_LOCATION_IMPORT_HISTORIC"
        )

    context.manifest_s3_input_location_export_prefix = (
        manifest_comparison_helper.generate_s3_prefix_for_manifest_input_files(
            context.config.userdata.get("MANIFEST_S3_INPUT_LOCATION_EXPORT"),
            context.manifest_snapshot_type,
        )
    )

    context.manifest_s3_output_parquet_location = (
        manifest_comparison_helper.generate_s3_prefix_for_manifest_output_files(
            context.config.userdata.get("MANIFEST_S3_INPUT_PARQUET_LOCATION"),
            context.manifest_import_type,
            context.manifest_snapshot_type,
        )
    )
    context.manifest_s3_input_parquet_location_combined = "s3://" + os.path.join(
        context.manifest_s3_bucket,
        context.manifest_s3_output_parquet_location,
        "combined",
    )
    context.manifest_s3_input_parquet_location_missing_import = "s3://" + os.path.join(
        context.manifest_s3_bucket,
        context.manifest_s3_output_parquet_location,
        "missing_import",
    )
    context.manifest_s3_input_parquet_location_missing_export = "s3://" + os.path.join(
        context.manifest_s3_bucket,
        context.manifest_s3_output_parquet_location,
        "missing_export",
    )
    context.manifest_s3_input_parquet_location_counts = "s3://" + os.path.join(
        context.manifest_s3_bucket,
        context.manifest_s3_output_parquet_location,
        "counts",
    )
    context.manifest_s3_input_parquet_location_mismatched_timestamps = (
        "s3://"
        + os.path.join(
            context.manifest_s3_bucket,
            context.manifest_s3_output_parquet_location,
            "mismatched_timestamps",
        )
    )
    context.manifest_s3_output_prefix = (
        manifest_comparison_helper.generate_s3_prefix_for_manifest_output_files(
            context.config.userdata.get("MANIFEST_S3_OUTPUT_LOCATION"),
            context.manifest_import_type,
            context.manifest_snapshot_type,
        )
    )
    context.manifest_s3_output_prefix_queries = os.path.join(
        context.manifest_s3_output_prefix, "queries"
    )
    context.manifest_s3_output_prefix_results = os.path.join(
        context.manifest_s3_output_prefix, "results"
    )
    context.manifest_s3_output_prefix_templates = os.path.join(
        context.manifest_s3_output_prefix, "templates"
    )
    context.manifest_s3_output_location_templates = "s3://" + os.path.join(
        context.manifest_s3_bucket, context.manifest_s3_output_prefix_templates
    )
    context.manifest_s3_output_location_queries = "s3://" + os.path.join(
        context.manifest_s3_bucket, context.manifest_s3_output_prefix_queries
    )

    manifest_start_raw = context.config.userdata.get(
        "MANIFEST_COMPARISON_CUT_OFF_DATE_START"
    )
    context.manifest_cut_off_date_start_epoch = (
        0
        if not manifest_start_raw
        else date_helper.generate_milliseconds_epoch_from_timestamp(manifest_start_raw)
    )
    context.manifest_cut_off_date_end_epoch = (
        date_helper.generate_milliseconds_epoch_from_timestamp(
            context.config.userdata.get("MANIFEST_COMPARISON_CUT_OFF_DATE_END")
        )
    )
    context.manifest_margin_of_error_epoch = (
        date_helper.generate_milliseconds_epoch_from_timestamp(
            context.config.userdata.get("MANIFEST_COMPARISON_CUT_OFF_DATE_END"),
            int(
                context.config.userdata.get(
                    "MANIFEST_COMPARISON_MARGIN_OF_ERROR_MINUTES"
                )
            ),
        )
    )

    context.manifest_verify_results = context.config.userdata.get(
        "MANIFEST_COMPARISON_VERIFY_RESULTS"
    )
    context.manifest_report_count_of_ids = int(
        context.config.userdata.get("MANIFEST_REPORT_COUNT_OF_IDS")
    )

    context.manifest_etl_glue_job_name = context.config.userdata.get(
        "MANIFEST_ETL_GLUE_JOB_NAME"
    )

    context.manifest_database_name = context.config.userdata.get(
        "MANIFEST_COMPARISON_DATABASE_NAME"
    )
    context.manifest_missing_imports_table_name = (
        manifest_comparison_helper.generate_manifest_table_name(
            context.manifest_database_name,
            context.config.userdata.get(
                "MANIFEST_COMPARISON_TABLE_NAME_MISSING_IMPORTS_PARQUET"
            ),
            context.manifest_import_type,
            context.manifest_snapshot_type,
        )
    )
    context.manifest_missing_exports_table_name = (
        manifest_comparison_helper.generate_manifest_table_name(
            context.manifest_database_name,
            context.config.userdata.get(
                "MANIFEST_COMPARISON_TABLE_NAME_MISSING_EXPORTS_PARQUET"
            ),
            context.manifest_import_type,
            context.manifest_snapshot_type,
        )
    )
    context.manifest_counts_table_name = (
        manifest_comparison_helper.generate_manifest_table_name(
            context.manifest_database_name,
            context.config.userdata.get(
                "MANIFEST_COMPARISON_TABLE_NAME_COUNTS_PARQUET"
            ),
            context.manifest_import_type,
            context.manifest_snapshot_type,
        )
    )
    context.manifest_mismatched_timestamps_table_name = (
        manifest_comparison_helper.generate_manifest_table_name(
            context.manifest_database_name,
            context.config.userdata.get(
                "MANIFEST_COMPARISON_TABLE_NAME_MISMATCHED_TIMESTAMPS_PARQUET"
            ),
            context.manifest_import_type,
            context.manifest_snapshot_type,
        )
    )


def generate_test_run_topics(context):
    """Generates the topics in use for this test run.

    Keyword arguments:
    context -- the behave context object
    """
    console_printer.print_info(f"Generating the topics for this test run")

    if context.config.userdata.get("IS_SYNTHETIC_DATA_INGESTION"):
        context.topics = template_helper.generate_synthetic_data_topic_names(
            context.synthetic_rawdata_prefix, context.aws_datasets_bucket
        )
    else:
        context.topics = template_helper.generate_topic_names(
            context.test_run_name,
            int(context.number_of_topics_to_use),
            context.db_name,
            False,
        )
        context.topics_delimited = ",".join(context.topics)
        context.topics_unique = template_helper.generate_topic_names(
            context.test_run_name,
            int(context.number_of_topics_to_use),
            context.db_name,
            True,
        )
        context.topics_unique_delimited = ",".join(context.topics_unique)

    console_printer.print_info(f"Generated the topics for this test run")
