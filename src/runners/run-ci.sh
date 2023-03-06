#!/bin/sh

META_FOLDER="${1}"
LOCAL_PROFILE="${2}" # Only used for local running

arn_value="arn"
aws_value="aws"
service="iam"

if [[ ! "${LOCAL_PROFILE}" == *"-profile"* ]]; then
    export LOCAL_PROFILE="" # Backwards compatability with existing pipeline calls
fi

if [[ ! -z "${META_FOLDER_OVERRIDE}" ]]; then
    export META_FOLDER="${META_FOLDER_OVERRIDE}" # Allow for direct running as a container in CI
fi

# shellcheck disable=SC2112
function get_secret_values() {
    secret_value=$(aws secretsmanager get-secret-value --secret-id  "/concourse/dataworks/end-to-end" ${LOCAL_PROFILE} | jq -r '.SecretBinary' | base64 --decode)
    s3_bucket=$(echo "$secret_value" | jq -r '.["terraform-outputs"].s3_bucket')
    s3_prefix=$(echo "$secret_value" | jq -r '.["terraform-outputs"].s3_prefix')
    account_number=$(echo "$secret_value" | jq -r '.["terraform-outputs"].account_number // empty')

    if [[ ! -z "${account_number}" && "${OVERRIDE_ROLE}" == "true" ]]; then
        # Set for local running
        export AWS_ROLE_ARN="${arn_value}:${aws_value}:${service}::${account_number}:role/administrator"
        export AWS_ACC=${account_number}
    fi

    export CONFIG_LOCATION="s3://${s3_bucket}/${s3_prefix}/"
}

# shellcheck disable=SC2112
function download_config() {
    echo "Downloading the config files from S3 for the environment under test"

    s3_location="${1}"
    local_file_folder="${2}"

    echo "Clear or make local config directory"
    if [[ ! -d "${local_file_folder}" ]]; then
        mkdir "${local_file_folder}"
    else
        rm -rf "${local_file_folder}"
    fi

    aws s3 cp ${s3_location} ${local_file_folder} --recursive ${LOCAL_PROFILE}
}

# shellcheck disable=SC2112
function delete_config() {
    echo "Deleting locally store config files"

    rm -rf ${1}
}

# shellcheck disable=SC2112
function set_file_locations() {
    echo "Setting the specific file locations from locally downloaded config files"

    local_file_location="${1}"

    export TF_INGEST_OUTPUT_FILE="${local_file_location}/aws-ingestion.json"
    export TF_INTERNAL_COMPUTE_OUTPUT_FILE="${local_file_location}/aws-internal-compute.json"
    export TF_SNAPSHOT_SENDER_OUTPUT_FILE="${local_file_location}/aws-snapshot-sender.json"
    export TF_MGMT_OUTPUT_FILE="${local_file_location}/aws-management-infrastructure.json"
    export TF_ADG_OUTPUT_FILE="${local_file_location}/aws-analytical-dataset-generation.json"
    export TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE="${local_file_location}/dataworks-aws-ingest-consumers.json"
    export TF_UCFS_CLAIMANT_OUTPUT_FILE="${local_file_location}/ucfs-claimant.json"
    export TF_DATAWORKS_AWS_UCFS_CLAIMANT_CONSUMER="${local_file_location}/dataworks-aws-ucfs-claimant-consumer.json"
    export TF_COMMON_OUTPUT_FILE="${local_file_location}/aws-common-infrastructure.json"
    export TF_DATAWORKS_AWS_INGESTION_ECS_CLUSTER="${local_file_location}/dataworks-aws-ingestion-ecs-cluster.json"
    export TF_DATAWORKS_STREAMS_KAFKA_PRODUCER_APP="${local_file_location}/dataworks-ml-streams-kafka-producer.json"
    export TF_DATAWORKS_STREAMS_KAFKA_CONSUMER_APP="${local_file_location}/dataworks-ml-streams-kafka-consumer.json"
    export TF_DATAWORKS_AWS_S3_OBJECT_TAGGER="${local_file_location}/dataworks-aws-s3-object-tagger.json"
}

# shellcheck disable=SC2112
function execute_behave() {
    echo "Executing tests against the environment under test"

    NOT_SET_FLAG="NOT_SET"

    if [[ ! -z "${TF_INGEST_OUTPUT_FILE}" && "${TF_INGEST_OUTPUT_FILE}" != "${NOT_SET_FLAG}" ]]; then
        echo "Using ${TF_INGEST_OUTPUT_FILE} ..."
        DLQ_S3_PATH_PREFIX="$(cat ${TF_INGEST_OUTPUT_FILE} | jq -r '.stub_ucfs_s3_dlq_prefix.value.prefix')"
        AWS_S3_INPUT_BUCKET="$(cat ${TF_INGEST_OUTPUT_FILE} | jq -r '.s3_buckets.value.input_bucket')"
        MONGO_DATA_LOAD_PREFIXES="NOT_SET"
        DYNAMODB_TABLE_NAME="$(cat ${TF_INGEST_OUTPUT_FILE} | jq -r '.stub_ucfs_job_status_table.value.name')"
        AWS_SNS_TOPIC_NAME="$(cat ${TF_INGEST_OUTPUT_FILE} | jq -r '.stub_ucfs_sns_topic_arn.value.name[0]')"
        UCFS_HISTORIC_DATA_PREFIX="$(cat ${TF_INGEST_OUTPUT_FILE} | jq -r '.ucfs_historic_data_prefix.value')"
        PLAINTEXT_ENCRYPTION_KEY="$(cat ${TF_INGEST_OUTPUT_FILE} | jq -r '.test_encryption_keys.value.plaintext')"
        ENCRYPTED_ENCRYPTION_KEY="$(cat ${TF_INGEST_OUTPUT_FILE} | jq -r '.test_encryption_keys.value.encrypted')"
        MASTER_KEY_ID="$(cat ${TF_INGEST_OUTPUT_FILE} | jq -r '.test_encryption_keys.value.master')"
        MANIFEST_S3_OUTPUT_LOCATION="$(cat ${TF_INGEST_OUTPUT_FILE} | jq -r '.manifest_comparison_parameters.value.query_output_s3_prefix')"
        MANIFEST_S3_INPUT_LOCATION_IMPORT_STREAMING_MAIN="$(cat ${TF_INGEST_OUTPUT_FILE} | jq -r '.manifest_comparison_parameters.value.streaming_folder_main')"
        MANIFEST_S3_INPUT_LOCATION_IMPORT_STREAMING_EQUALITY="$(cat ${TF_INGEST_OUTPUT_FILE} | jq -r '.manifest_comparison_parameters.value.streaming_folder_equality')"

        ASG_MAX_COUNT_KAFKA_STUB="$(cat ${TF_INGEST_OUTPUT_FILE} | jq -r '.asg_properties.value.max_counts.stub_ucfs_kafka_broker // empty')"
        ASG_PREFIX_KAFKA_STUB="$(cat ${TF_INGEST_OUTPUT_FILE} | jq -r '.asg_properties.value.prefixes.stub_ucfs_kafka_broker // empty')"

        K2HB_MAIN_MANIFEST_WRITE_S3_PREFIX="$(cat ${TF_INGEST_OUTPUT_FILE} | jq -r '.k2hb_manifest_write_locations.value.main_prefix // empty')"
        K2HB_EQUALITY_MANIFEST_WRITE_S3_PREFIX="$(cat ${TF_INGEST_OUTPUT_FILE} | jq -r '.k2hb_manifest_write_locations.value.equality_prefix // empty')"
        K2HB_AUDIT_MANIFEST_WRITE_S3_PREFIX="$(cat ${TF_INGEST_OUTPUT_FILE} | jq -r '.k2hb_manifest_write_locations.value.audit_prefix // empty')"

        METADATA_STORE_TABLE_NAME_UCFS="$(cat ${TF_INGEST_OUTPUT_FILE} | jq -r '.metadata_store_table_names.value.ucfs')"
        METADATA_STORE_TABLE_NAME_EQUALITIES="$(cat ${TF_INGEST_OUTPUT_FILE} | jq -r '.metadata_store_table_names.value.equality')"
        METADATA_STORE_TABLE_NAME_AUDIT="$(cat ${TF_INGEST_OUTPUT_FILE} | jq -r '.metadata_store_table_names.value.audit')"

        CDL_RUN_SCRIPT_S3_URL="$(cat ${TF_INGEST_OUTPUT_FILE} | jq -r '.corporate_data_loader.value.run_cdl_s3_url')"
        CDL_SPLIT_INPUTS_S3_URL="$(cat ${TF_INGEST_OUTPUT_FILE} | jq -r '.corporate_data_loader.value.cdl_split_inputs_s3_url')"

        HDL_RUN_SCRIPT_S3_URL="$(cat ${TF_INGEST_OUTPUT_FILE} | jq -r '.historic_data_loader.value.run_hdl_s3_url')"

        CDL_DATA_LOAD_S3_BASE_PREFIX="$(cat ${TF_INGEST_OUTPUT_FILE} | jq -r '.corporate_data_loader.value.s3_base_prefix')"
        HDL_DATA_LOAD_S3_BASE_PREFIX="$(cat ${TF_INGEST_OUTPUT_FILE} | jq -r '.ucfs_historic_data_prefix.value')"
        CDL_DATA_LOAD_FILE_PATTERN_UCFS="$(cat ${TF_INGEST_OUTPUT_FILE} | jq -r '.corporate_data_loader.value.file_pattern_ucfs')"

        CORPORATE_STORAGE_S3_BUCKET_ID="$(cat ${TF_INGEST_OUTPUT_FILE} | jq -r '.corporate_storage_bucket.value.id')"
    else
        echo "Skipping TF_INGEST_OUTPUT_FILE=${TF_INGEST_OUTPUT_FILE}"
    fi

    if [[ ! -z "${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE}" && "${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE}" != "${NOT_SET_FLAG}" ]]; then
        echo "Using ${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE} ..."
        MANIFEST_ETL_GLUE_JOB_NAME="$(cat ${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE} | jq -r '.manifest_etl.value.job_name_combined')"
        MANIFEST_COMPARISON_DATABASE_NAME="$(cat ${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE} | jq -r '.manifest_etl.value.database_name')"
        MANIFEST_COMPARISON_TABLE_NAME_MISSING_IMPORTS_PARQUET="$(cat ${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE} | jq -r '.manifest_etl.value.table_name_missing_imports_parquet')"
        MANIFEST_COMPARISON_TABLE_NAME_MISSING_EXPORTS_PARQUET="$(cat ${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE} | jq -r '.manifest_etl.value.table_name_missing_exports_parquet')"
        MANIFEST_COMPARISON_TABLE_NAME_COUNTS_PARQUET="$(cat ${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE} | jq -r '.manifest_etl.value.table_name_counts_parquet')"
        MANIFEST_COMPARISON_TABLE_NAME_MISMATCHED_TIMESTAMPS_PARQUET="$(cat ${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE} | jq -r '.manifest_etl.value.table_name_mismatched_timestamps_parquet')"
        RECONCILER_MAIN_CLUSTER_NAME="$(cat ${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE} | jq -r '.k2hb_reconciliation.value.cluster_names.ucfs_reconciliation // empty')"
        RECONCILER_EQUALITIES_CLUSTER_NAME="$(cat ${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE} | jq -r '.k2hb_reconciliation.value.cluster_names.equality_reconciliation // empty')"
        RECONCILER_AUDIT_CLUSTER_NAME="$(cat ${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE} | jq -r '.k2hb_reconciliation.value.cluster_names.audit_reconciliation // empty')"
        RECONCILER_MAIN_SERVICE_NAME="$(cat ${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE} | jq -r '.k2hb_reconciliation.value.service_names.ucfs_reconciliation // empty')"
        RECONCILER_EQUALITIES_SERVICE_NAME="$(cat ${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE} | jq -r '.k2hb_reconciliation.value.service_names.equality_reconciliation // empty')"
        RECONCILER_EQUALITIES_SERVICE_NAME="$(cat ${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE} | jq -r '.k2hb_reconciliation.value.service_names.audit_reconciliation // empty')"
        RECONCILER_AUDIT_SERVICE_NAME="$(cat ${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE} | jq -r '.k2hb_reconciliation.value.service_names.audit_reconciliation // empty')"
        RECONCILER_MAIN_DESIRED_TASK_COUNT="$(cat ${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE} | jq -r '.k2hb_reconciliation.value.task_counts.ucfs_reconciliation // empty')"
        RECONCILER_EQUALITIES_DESIRED_TASK_COUNT="$(cat ${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE} | jq -r '.k2hb_reconciliation.value.task_counts.equality_reconciliation // empty')"
        RECONCILER_AUDIT_DESIRED_TASK_COUNT="$(cat ${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE} | jq -r '.k2hb_reconciliation.value.task_counts.audit_reconciliation // empty')"
        ASG_MAX_COUNT_K2HB_MAIN_LONDON="$(cat ${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE} | jq -r '.asg_properties.value.max_counts.k2hb_main_london // empty')"
        ASG_MAX_COUNT_K2HB_MAIN_DEDICATED_LONDON="$(cat ${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE} | jq -r '.asg_properties.value.max_counts.k2hb_main_dedicated_london // empty')"
        ASG_MAX_COUNT_K2HB_EQUALITY_LONDON="$(cat ${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE} | jq -r '.asg_properties.value.max_counts.k2hb_equality_london // empty')"
        ASG_MAX_COUNT_K2HB_AUDIT_LONDON="$(cat ${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE} | jq -r '.asg_properties.value.max_counts.k2hb_audit_london // empty')"
        ASG_PREFIX_K2HB_MAIN_LONDON="$(cat ${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE} | jq -r '.asg_properties.value.prefixes.k2hb_main_london // empty')"
        ASG_PREFIX_K2HB_MAIN_DEDICATED_LONDON="$(cat ${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE} | jq -r '.asg_properties.value.prefixes.k2hb_main_dedicated_london // empty')"
        ASG_PREFIX_K2HB_EQUALITY_LONDON="$(cat ${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE} | jq -r '.asg_properties.value.prefixes.k2hb_equality_london // empty')"
        ASG_PREFIX_K2HB_AUDIT_LONDON="$(cat ${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE} | jq -r '.asg_properties.value.prefixes.k2hb_audit_london // empty')"
    else
        echo "Skipping TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE=${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE}"
    fi

    if [[ ! -z "${TF_DATAWORKS_AWS_INGESTION_ECS_CLUSTER}" && "${TF_DATAWORKS_AWS_INGESTION_ECS_CLUSTER}" != "${NOT_SET_FLAG}" ]]; then
        echo "Using ${TF_DATAWORKS_AWS_INGESTION_ECS_CLUSTER} ..."
        ASG_MAX_COUNT_INGESTION_ECS_CLUSTER="$(cat ${TF_DATAWORKS_AWS_INGESTION_ECS_CLUSTER} | jq -r '.ingestion_ecs_cluster_autoscaling_group.value.max_size // empty')"
        ASG_PREFIX_INGESTION_ECS_CLUSTER="$(cat ${TF_DATAWORKS_AWS_INGESTION_ECS_CLUSTER} | jq -r '.ingestion_ecs_cluster_autoscaling_group.value.name_prefix // empty')"
        echo "ASG_MAX_COUNT_INGESTION_ECS_CLUSTER=${ASG_MAX_COUNT_INGESTION_ECS_CLUSTER}"
        echo "ASG_PREFIX_INGESTION_ECS_CLUSTER=${ASG_PREFIX_INGESTION_ECS_CLUSTER}"
    else
        echo "Skipping TF_DATAWORKS_AWS_INGESTION_ECS_CLUSTER=${TF_DATAWORKS_AWS_INGESTION_ECS_CLUSTER}"
    fi

    if [[ ! -z "${TF_INTERNAL_COMPUTE_OUTPUT_FILE}" && "${TF_INTERNAL_COMPUTE_OUTPUT_FILE}" != "${NOT_SET_FLAG}" ]]; then
        echo "Using ${TF_INTERNAL_COMPUTE_OUTPUT_FILE} ..."
        AWS_SNS_UC_ECC_ARN="$(cat ${TF_INTERNAL_COMPUTE_OUTPUT_FILE} | jq -r '.uc_export_to_crown_controller_messages_sns_topic.value.arn')"
        AWS_SQS_HISTORIC_DATA_IMPORTER_QUEUE_URL="$(cat ${TF_INTERNAL_COMPUTE_OUTPUT_FILE} | jq -r '.sqs_queues.value.historic_importer_url')"
        AWS_SQS_SNAPSHOT_SENDER_QUEUE_URL="$(cat ${TF_INTERNAL_COMPUTE_OUTPUT_FILE} | jq -r '.sqs_queues.value.htme_outgoing_url')"
        MONGO_DATA_LOAD_PREFIXES="$(cat ${TF_INTERNAL_COMPUTE_OUTPUT_FILE} | jq -r '.ucfs_data_load_prefixes.value.prefixes_csv')"

        ASG_MAX_COUNT_HTME="$(cat ${TF_INTERNAL_COMPUTE_OUTPUT_FILE} | jq -r '.asg_properties.value.max_counts.htme // empty')"
        ASG_MAX_COUNT_HDI="$(cat ${TF_INTERNAL_COMPUTE_OUTPUT_FILE} | jq -r '.asg_properties.value.max_counts.hdi // empty')"
        ASG_PREFIX_HTME="$(cat ${TF_INTERNAL_COMPUTE_OUTPUT_FILE} | jq -r '.asg_properties.value.prefixes.htme // empty')"
        ASG_PREFIX_HDI="$(cat ${TF_INTERNAL_COMPUTE_OUTPUT_FILE} | jq -r '.asg_properties.value.prefixes.hdi // empty')"

        DYNAMO_DB_EXPORT_STATUS_TABLE_NAME="$(cat ${TF_INTERNAL_COMPUTE_OUTPUT_FILE} | jq -r '.uc_export_crown_dynamodb_table.value.name')"
        DYNAMO_DB_PRODUCT_STATUS_TABLE_NAME="$(cat ${TF_INTERNAL_COMPUTE_OUTPUT_FILE} | jq -r '.data_pipeline_metadata_dynamo.value.name')"
        HTME_DEFAULT_TOPIC_LIST_FULL_CONTENT="$(cat ${TF_INTERNAL_COMPUTE_OUTPUT_FILE} | jq -r '.htme_default_topics_csv.value.full.content')"
        HTME_DEFAULT_TOPIC_LIST_INCREMENTALS_CONTENT="$(cat ${TF_INTERNAL_COMPUTE_OUTPUT_FILE} | jq -r '.htme_default_topics_csv.value.incrementals.content')"
        HTME_DEFAULT_TOPIC_LIST_DRIFT_TESTING_INCREMENTALS_CONTENT="$(cat ${TF_INTERNAL_COMPUTE_OUTPUT_FILE} | jq -r '.htme_default_topics_csv.value.drift_testing_incrementals.content')"

        MANIFEST_S3_BUCKET="$(cat ${TF_INTERNAL_COMPUTE_OUTPUT_FILE} | jq -r '.manifest_bucket.value.id')"
        MANIFEST_S3_INPUT_LOCATION_IMPORT_HISTORIC="$(cat ${TF_INTERNAL_COMPUTE_OUTPUT_FILE} | jq -r '.manifest_comparison_parameters.value.historic_folder')"
        MANIFEST_S3_INPUT_LOCATION_EXPORT="$(cat ${TF_INTERNAL_COMPUTE_OUTPUT_FILE} | jq -r '.manifest_s3_prefixes.value.export')"

        INGEST_HBASE_EMR_CLUSTER_ID="$(cat ${TF_INTERNAL_COMPUTE_OUTPUT_FILE} | jq -r '.aws_emr_cluster.value.cluster_id')"
        INGEST_HBASE_EMR_CLUSTER_ROOT_S3_BUCKET_ID="$(cat ${TF_INTERNAL_COMPUTE_OUTPUT_FILE} | jq -r '.aws_emr_cluster.value.root_bucket')"
        INGEST_HBASE_EMR_CLUSTER_ROOT_S3_ROOT_DIRECTORY="$(cat ${TF_INTERNAL_COMPUTE_OUTPUT_FILE} | jq -r '.aws_emr_cluster.value.root_directory')"

        MONITORING_SNS_TOPIC_ARN="$(cat ${TF_INTERNAL_COMPUTE_OUTPUT_FILE} | jq -r '.sns_topics.value.london_monitoring.arn')"
        MONGO_SNAPSHOT_BUCKET="$(cat ${TF_INTERNAL_COMPUTE_OUTPUT_FILE} | jq -r '.htme_s3_bucket.value.id')"
        MONGO_SNAPSHOT_PATH="$(cat ${TF_INTERNAL_COMPUTE_OUTPUT_FILE} | jq -r '.htme_s3_folder.value.id')"

        MANIFEST_S3_INPUT_PARQUET_LOCATION="$(cat ${TF_INTERNAL_COMPUTE_OUTPUT_FILE} | jq -r '.manifest_s3_prefixes.value.parquet')"

        CREATE_HBASE_TABLES_SCRIPT_S3_URL="$(cat ${TF_INTERNAL_COMPUTE_OUTPUT_FILE} | jq -r '.emr_ingest_hbase_files.value.create_hbase_tables_script')"
    else
        echo "Skipping TF_INTERNAL_COMPUTE_OUTPUT_FILE=${TF_INTERNAL_COMPUTE_OUTPUT_FILE}"
    fi

    if [[ ! -z "${TF_SNAPSHOT_SENDER_OUTPUT_FILE}" && "${TF_SNAPSHOT_SENDER_OUTPUT_FILE}" != "${NOT_SET_FLAG}" ]]; then
        echo "Using ${TF_SNAPSHOT_SENDER_OUTPUT_FILE} ..."
        ASG_PREFIX_SNAPSHOT_SENDER="$(cat ${TF_SNAPSHOT_SENDER_OUTPUT_FILE} | jq -r '.asg_properties.value.prefixes.snapshot_sender // empty')"

        SNAPSHOT_S3_OUTPUT_PATH="$(cat ${TF_SNAPSHOT_SENDER_OUTPUT_FILE} | jq -r '.stub_hdfs.value.path_prefix')"
        SNAPSHOT_S3_OUTPUT_BUCKET="$(cat ${TF_SNAPSHOT_SENDER_OUTPUT_FILE} | jq -r '.stub_hdfs.value.bucket_id[0]')"
        SNAPSHOT_S3_STATUS_PATH="$(cat ${TF_SNAPSHOT_SENDER_OUTPUT_FILE} | jq -r '.snapshot_sender_status_folder.value.s3_prefix')"
    else
        echo "Skipping TF_SNAPSHOT_SENDER_OUTPUT_FILE=${TF_SNAPSHOT_SENDER_OUTPUT_FILE}"
    fi

    if [[ ! -z "${TF_MGMT_OUTPUT_FILE}" && "${TF_MGMT_OUTPUT_FILE}" != "NOT_SET" ]]; then
        AWS_DATASETS_BUCKET="$(cat ${TF_MGMT_OUTPUT_FILE} |  jq -r '.datasets_bucket.value.id')"
    else
        echo "Skipping TF_MGMT_OUTPUT_FILE=${TF_MGMT_OUTPUT_FILE}"
    fi

    if [[ ! -z "${TF_COMMON_OUTPUT_FILE}" && "${TF_COMMON_OUTPUT_FILE}" != "NOT_SET"  ]]; then
        AWS_PUBLISHED_BUCKET="$(cat ${TF_COMMON_OUTPUT_FILE} |  jq -r '.published_bucket.value.id')"
        AWS_DATA_INGRESS_STAGE_BUCKET="$(cat ${TF_COMMON_OUTPUT_FILE} | jq -r '.data_ingress_stage_bucket.value.id')"
        AWS_PROCESSED_BUCKET="$(cat ${TF_COMMON_OUTPUT_FILE} |  jq -r '.processed_bucket.value.id')"
        AWS_REGION_MAIN="$(cat ${TF_COMMON_OUTPUT_FILE} |  jq -r '.region_names.value.london')"
        AWS_REGION_ALTERNATIVE="$(cat ${TF_COMMON_OUTPUT_FILE} |  jq -r '.region_names.value.ireland')"
        ASG_MAX_COUNT_SNAPSHOT_SENDER="$(cat ${TF_COMMON_OUTPUT_FILE} | jq -r '.snapshot_sender_max_size.value // empty')"
        DATAWORKS_MODEL_OUTPUT_BUCKET="$(cat ${TF_COMMON_OUTPUT_FILE} |  jq -r '.dataworks_model_processed_bucket.value.id')"
        DATAWORKS_MODEL_OUTPUT_SQS="$(cat ${TF_COMMON_OUTPUT_FILE} |  jq -r '.dataworks_model_processed_sqs.value.name')"
        DATAWORKS_DLQ_OUTPUT_BUCKET="$(cat ${TF_COMMON_OUTPUT_FILE} |  jq -r '.dataworks_model_dlq_output_bucket.value.id')"
        DATAWORKS_COMMON_CONFIG_BUCKET="$(cat ${TF_COMMON_OUTPUT_FILE} |  jq -r '.config_bucket.value.id')"
    else
        echo "Skipping TF_COMMON_OUTPUT_FILE=${TF_COMMON_OUTPUT_FILE}"
    fi

    if [[ ! -z "${TF_UCFS_CLAIMANT_OUTPUT_FILE}" && "${TF_UCFS_CLAIMANT_OUTPUT_FILE}" != "NOT_SET"  ]]; then
        UCFS_CLAIMANT_DOMAIN_NAME="$(cat ${TF_UCFS_CLAIMANT_OUTPUT_FILE} |  jq -r '.aws_api_gateway_base_path_mapping_ucfs_claimant.value.domain_name')"
        UCFS_CLAIMANT_API_USE_LONDON="$(cat ${TF_COMMON_OUTPUT_FILE} |  jq -r '.ucfs_claimant_api_gateway_use_london.value')"
        if [[ "${UCFS_CLAIMANT_API_USE_LONDON}" == "true" ]]; then
          UCFS_CLAIMANT_API_ACTIVE_REGION='London'
        else
          UCFS_CLAIMANT_API_ACTIVE_REGION='Ireland'
        fi
        UCFS_CLAIMANT_API_PATH_V1_GET_AWARD_DETAILS="$(cat ${TF_UCFS_CLAIMANT_OUTPUT_FILE} |  jq -r '.aws_api_gateway_resource_v1_getAwardDetails.value.path')"
        UCFS_CLAIMANT_API_PATH_V2_GET_AWARD_DETAILS="$(cat ${TF_UCFS_CLAIMANT_OUTPUT_FILE} |  jq -r '.aws_api_gateway_resource_v2_getAwardDetails.value.path')"
        UCFS_CLAIMANT_API_SALT_SSM_PARAMETER_NAME="$(cat ${TF_UCFS_CLAIMANT_OUTPUT_FILE} |  jq -r '.nino_salt_london_ssm_param.value')"
    else
        echo "Skipping TF_UCFS_CLAIMANT_OUTPUT_FILE=${TF_UCFS_CLAIMANT_OUTPUT_FILE}"
    fi

    if [[ ! -z "${TF_DATAWORKS_AWS_UCFS_CLAIMANT_CONSUMER}" && "${TF_DATAWORKS_AWS_UCFS_CLAIMANT_CONSUMER}" != "NOT_SET"  ]]; then
        UCFS_CLAIMANT_API_KAFKA_CONSUMER_CLUSTER_NAME="$(cat ${TF_DATAWORKS_AWS_UCFS_CLAIMANT_CONSUMER} |  jq -r '.claimant_api_kafka_consumer.value.cluster_name')"
        UCFS_CLAIMANT_API_KAFKA_CONSUMER_SERVICE_NAME="$(cat ${TF_DATAWORKS_AWS_UCFS_CLAIMANT_CONSUMER} |  jq -r '.claimant_api_kafka_consumer.value.service_name')"
        UCFS_CLAIMANT_API_KAFKA_CONSUMER_SERVICE_DESIRED_TASK_COUNT="$(cat ${TF_DATAWORKS_AWS_UCFS_CLAIMANT_CONSUMER} |  jq -r '.claimant_api_kafka_consumer.value.task_count')"
    else
        echo "Skipping TF_DATAWORKS_AWS_UCFS_CLAIMANT_CONSUMER=${TF_DATAWORKS_AWS_UCFS_CLAIMANT_CONSUMER}"
    fi

    if [[ ! -z "${TF_DATAWORKS_STREAMS_KAFKA_PRODUCER_APP}" && "${TF_DATAWORKS_STREAMS_KAFKA_PRODUCER_APP}" != "NOT_SET"  ]]; then
        echo "Using ${TF_DATAWORKS_STREAMS_KAFKA_PRODUCER_APP} ..."
        DATAWORKS_STREAMS_KAFKA_PRODUCER="$(cat ${TF_DATAWORKS_STREAMS_KAFKA_PRODUCER_APP} |  jq -r '.dataworks_kafka_producer.value')"
        DATAWORKS_STREAMS_KAFKA_PRODUCER_HSM_KEY_ID="$(cat ${TF_DATAWORKS_STREAMS_KAFKA_PRODUCER_APP} |  jq -r '.hsm_key_id.value')"
        DATAWORKS_STREAMS_KAFKA_PRODUCER_HSM_PUB_KEY="$(cat ${TF_DATAWORKS_STREAMS_KAFKA_PRODUCER_APP} |  jq -r '.hsm_pub_key.value')"
    else
        echo "Skipping TF_DATAWORKS_STREAMS_KAFKA_PRODUCER_APP=${TF_DATAWORKS_STREAMS_KAFKA_PRODUCER_APP}"
    fi

    if [[ ! -z "${TF_DATAWORKS_STREAMS_KAFKA_CONSUMER_APP}" && "${TF_DATAWORKS_STREAMS_KAFKA_CONSUMER_APP}" != "NOT_SET"  ]]; then
        echo "Using ${TF_DATAWORKS_STREAMS_KAFKA_CONSUMER_APP} ..."
        DATAWORKS_STREAMS_KAFKA_DLQ_CONSUMER="$(cat ${TF_DATAWORKS_STREAMS_KAFKA_CONSUMER_APP} |  jq -r '.dataworks_dlq_consumer.value')"
    else
        echo "Skipping TF_DATAWORKS_STREAMS_KAFKA_CONSUMER_APP=${TF_DATAWORKS_STREAMS_KAFKA_CONSUMER_APP}"
    fi

    if [[ ! -z "${TF_DATAWORKS_AWS_S3_OBJECT_TAGGER}" && "${TF_DATAWORKS_AWS_S3_OBJECT_TAGGER}" != "${NOT_SET_FLAG}" ]]; then
        echo "Using ${TF_DATAWORKS_AWS_S3_OBJECT_TAGGER} ..."
        UC_FEATURE_DATA_CLASSIFICATION="$(cat ${TF_DATAWORKS_AWS_S3_OBJECT_TAGGER} | jq -r ".uc_feature_object_tagger_data_classification.value")"
        PDM_DATA_CLASSIFICATION="$(cat ${TF_DATAWORKS_AWS_S3_OBJECT_TAGGER} | jq -r ".pdm_object_tagger_data_classification.value")"
    else
        echo "Skipping TF_DATAWORKS_AWS_S3_OBJECT_TAGGER=${TF_DATAWORKS_AWS_S3_OBJECT_TAGGER}"
    fi



    if [[ -z "${TEST_RUN_NAME}" ]]; then
        if [[ -f $META_FOLDER/build-pipeline-name ]]; then
            PIPELINE_NAME=$(cat "$META_FOLDER/build-pipeline-name")
        elif [[ -f $META_FOLDER/build_pipeline_name ]]; then
            PIPELINE_NAME=$(cat "$META_FOLDER/build_pipeline_name")
        else
            PIPELINE_NAME=$(uuidgen)
        fi

        if [[ -f $META_FOLDER/build_job_name ]]; then
            JOB_NAME=$(cat "$META_FOLDER/build_job_name")
        else
            JOB_NAME=$(cat "$META_FOLDER/build_job_name")
        fi

        if [[ -f $META_FOLDER/build-name ]]; then
            BUILD_NUMBER=$(cat "$META_FOLDER/build-name")
            UNIQUE_JOB_NAME="${JOB_NAME}_${BUILD_NUMBER}"
        elif [[ -f $META_FOLDER/build_name ]]; then
            BUILD_NUMBER=$(cat "$META_FOLDER/build_name")
            UNIQUE_JOB_NAME="${JOB_NAME}_${BUILD_NUMBER}"
        else
            UNIQUE_JOB_NAME="${JOB_NAME}"
        fi

        if [[ -z "${UNIQUE_JOB_NAME}" ]]; then
            UNIQUE_JOB_NAME=$(uuidgen)
        fi

        TEST_RUN_NAME="${PIPELINE_NAME}_${UNIQUE_JOB_NAME}"
    fi

    if [[ -z "${AWS_ROLE_ARN}" ]]; then
        AWS_ROLE_ARN="${arn_value}:${aws_value}:${service}::${AWS_ACC}:role/${AWS_DEFAULT_PROFILE}"
    fi

    if [[ -z "${AWS_SESSION_TIMEOUT_SECONDS}" ]]; then
        AWS_SESSION_TIMEOUT_SECONDS=7200
    fi

    FIXTURE_PATH_REMOTE="fixture-data/business-data/kafka/functional-tests"

    if [[ -z "${NUMBER_OF_TOPICS_TO_USE}" ]]; then
        NUMBER_OF_TOPICS_TO_USE=1
    fi

    if [[ -z "${E2E_TEST_TIMEOUT}" ]]; then
        E2E_TEST_TIMEOUT=600
    fi

    if [[ -z "${LOAD_TEST_KEY_METHOD}" ]]; then
        LOAD_TEST_KEY_METHOD="single"
    fi

    if [[ -z "${LOAD_TESTS_FILE_COUNT}" ]]; then
        LOAD_TESTS_FILE_COUNT=100
    fi

    if [[ -z "${LOAD_TESTS_RECORD_COUNT}" ]]; then
        LOAD_TESTS_RECORD_COUNT=100
    fi

    if [[ -z "${LOAD_TESTS_DATA_MAX_WORKER_COUNT}" ]]; then
        LOAD_TESTS_DATA_MAX_WORKER_COUNT=20
    fi

    if [[ -z "${MANIFEST_COMPARISON_CUT_OFF_DATE_START}" ]]; then
        MANIFEST_COMPARISON_CUT_OFF_DATE_START="1983-11-15T09:09:55.000"
    fi

    if [[ -z "${MANIFEST_COMPARISON_CUT_OFF_DATE_END}" ]]; then
        MANIFEST_COMPARISON_CUT_OFF_DATE_END="2099-11-15T09:09:55.000"
    fi

    if [[ -z "${MANIFEST_COMPARISON_MARGIN_OF_ERROR_MINUTES}" ]]; then
        MANIFEST_COMPARISON_MARGIN_OF_ERROR_MINUTES="2"
    fi

    if [[ -z "${MANIFEST_REPORT_COUNT_OF_IDS}" ]]; then
        MANIFEST_REPORT_COUNT_OF_IDS="10"
    fi

    if [[ -z "${MANIFEST_COMPARISON_SNAPSHOT_TYPE}" ]]; then
        MANIFEST_COMPARISON_SNAPSHOT_TYPE="full"
    fi

    if [[ -z "${MANIFEST_COMPARISON_IMPORT_TYPE}" ]]; then
        MANIFEST_COMPARISON_IMPORT_TYPE="historic"
    fi

    if [[ -z "${LOAD_TESTS_KAFKA_MESSAGE_VOLUME}" ]]; then
        LOAD_TESTS_KAFKA_MESSAGE_VOLUME="1"
    fi

    if [[ -z "${LOAD_TESTS_KAFKA_RANDOM_KEY}" ]]; then
        LOAD_TESTS_KAFKA_RANDOM_KEY="false"
    fi

    if [[ -z "${LOAD_TESTS_KAFKA_RECORD_COUNT}" ]]; then
        LOAD_TESTS_KAFKA_RECORD_COUNT="100"
    fi

    pip3 install -r ../../requirements.txt --trusted-host pypi.org --trusted-host files.pythonhosted.org --user

    cd ../

    set -x # Will ensure the below command is printed so can be run again if needed

    behave features --stop --quiet --no-logcapture --no-capture --no-capture-stderr --tags="~@wip" --tags="${E2E_FEATURE_TAG_FILTER}" \
    -D SUPPRESS_MONITORING_ALERTS="${SUPPRESS_MONITORING_ALERTS}" \
    -D E2E_FEATURE_TAG_FILTER="${E2E_FEATURE_TAG_FILTER}" \
    -D PLAINTEXT_ENCRYPTION_KEY="${PLAINTEXT_ENCRYPTION_KEY}" \
    -D ENCRYPTED_ENCRYPTION_KEY="${ENCRYPTED_ENCRYPTION_KEY}" \
    -D MASTER_KEY_ID="${MASTER_KEY_ID}" \
    -D UCFS_HISTORIC_DATA_PREFIX="${UCFS_HISTORIC_DATA_PREFIX}" \
    -D NUMBER_OF_TOPICS_TO_USE="${NUMBER_OF_TOPICS_TO_USE}" \
    -D LOAD_TEST_KEY_METHOD="${LOAD_TEST_KEY_METHOD}" \
    -D DLQ_S3_PATH_PREFIX="${DLQ_S3_PATH_PREFIX}" \
    -D DYNAMODB_TABLE_NAME="${DYNAMODB_TABLE_NAME}" \
    -D AWS_SNS_TOPIC_NAME="${AWS_SNS_TOPIC_NAME}" \
    -D AWS_S3_INPUT_BUCKET="${AWS_S3_INPUT_BUCKET}" \
    -D TEST_RUN_NAME="${TEST_RUN_NAME}" \
    -D FIXTURE_PATH_REMOTE="${FIXTURE_PATH_REMOTE}" \
    -D AWS_ACC="${AWS_ACC}" \
    -D E2E_TEST_TIMEOUT="${E2E_TEST_TIMEOUT}" \
    -D MONGO_SNAPSHOT_PATH="${MONGO_SNAPSHOT_PATH}" \
    -D MONGO_SNAPSHOT_BUCKET="${MONGO_SNAPSHOT_BUCKET}" \
    -D MONGO_DATA_LOAD_PREFIXES="${MONGO_DATA_LOAD_PREFIXES}" \
    -D AWS_SNS_UC_ECC_ARN="${AWS_SNS_UC_ECC_ARN}" \
    -D AWS_SQS_HISTORIC_DATA_IMPORTER_QUEUE_URL="${AWS_SQS_HISTORIC_DATA_IMPORTER_QUEUE_URL}" \
    -D AWS_SQS_SNAPSHOT_SENDER_QUEUE_URL="${AWS_SQS_SNAPSHOT_SENDER_QUEUE_URL}" \
    -D MANIFEST_COMPARISON_CUT_OFF_DATE_START="${MANIFEST_COMPARISON_CUT_OFF_DATE_START}" \
    -D MANIFEST_COMPARISON_CUT_OFF_DATE_END="${MANIFEST_COMPARISON_CUT_OFF_DATE_END}" \
    -D MANIFEST_COMPARISON_MARGIN_OF_ERROR_MINUTES="${MANIFEST_COMPARISON_MARGIN_OF_ERROR_MINUTES}" \
    -D MANIFEST_COMPARISON_SNAPSHOT_TYPE="${MANIFEST_COMPARISON_SNAPSHOT_TYPE}" \
    -D MANIFEST_COMPARISON_IMPORT_TYPE="${MANIFEST_COMPARISON_IMPORT_TYPE}" \
    -D MANIFEST_S3_BUCKET="${MANIFEST_S3_BUCKET}" \
    -D MANIFEST_S3_INPUT_LOCATION_IMPORT_HISTORIC="${MANIFEST_S3_INPUT_LOCATION_IMPORT_HISTORIC}" \
    -D MANIFEST_S3_INPUT_LOCATION_IMPORT_STREAMING_MAIN="${MANIFEST_S3_INPUT_LOCATION_IMPORT_STREAMING_MAIN}" \
    -D MANIFEST_S3_INPUT_LOCATION_IMPORT_STREAMING_EQUALITY="${MANIFEST_S3_INPUT_LOCATION_IMPORT_STREAMING_EQUALITY}" \
    -D MANIFEST_S3_INPUT_LOCATION_EXPORT="${MANIFEST_S3_INPUT_LOCATION_EXPORT}" \
    -D MANIFEST_S3_INPUT_PARQUET_LOCATION="${MANIFEST_S3_INPUT_PARQUET_LOCATION}" \
    -D MANIFEST_S3_OUTPUT_LOCATION="${MANIFEST_S3_OUTPUT_LOCATION}" \
    -D MANIFEST_COMPARISON_VERIFY_RESULTS="${MANIFEST_COMPARISON_VERIFY_RESULTS}" \
    -D LOAD_TESTS_FILE_COUNT="${LOAD_TESTS_FILE_COUNT}" \
    -D LOAD_TESTS_RECORD_COUNT="${LOAD_TESTS_RECORD_COUNT}" \
    -D LOAD_TESTS_DATA_MAX_WORKER_COUNT="${LOAD_TESTS_DATA_MAX_WORKER_COUNT}" \
    -D GENERATE_SNAPSHOTS_TOPICS_OVERRIDE="${GENERATE_SNAPSHOTS_TOPICS_OVERRIDE}" \
    -D SNAPSHOT_TYPE_OVERRIDE="${SNAPSHOT_TYPE_OVERRIDE}" \
    -D SEND_SNAPSHOTS_TOPICS_OVERRIDE="${SEND_SNAPSHOTS_TOPICS_OVERRIDE}" \
    -D SEND_SNAPSHOTS_DATE_OVERRIDE="${SEND_SNAPSHOTS_DATE_OVERRIDE}" \
    -D SEND_SNAPSHOTS_CORRELATION_ID_OVERRIDE="${SEND_SNAPSHOTS_CORRELATION_ID_OVERRIDE}" \
    -D GENERATE_SNAPSHOTS_CORRELATION_ID_OVERRIDE="${GENERATE_SNAPSHOTS_CORRELATION_ID_OVERRIDE}" \
    -D GENERATE_SNAPSHOTS_EXPORT_DATE_OVERRIDE="${GENERATE_SNAPSHOTS_EXPORT_DATE_OVERRIDE}" \
    -D SEND_SNAPSHOTS_REPROCESS_FILES_OVERRIDE="${SEND_SNAPSHOTS_REPROCESS_FILES_OVERRIDE}" \
    -D MANIFEST_ETL_GLUE_JOB_NAME="${MANIFEST_ETL_GLUE_JOB_NAME}" \
    -D ASG_MAX_COUNT_HTME="${ASG_MAX_COUNT_HTME}" \
    -D ASG_MAX_COUNT_HDI="${ASG_MAX_COUNT_HDI}" \
    -D ASG_MAX_COUNT_SNAPSHOT_SENDER="${ASG_MAX_COUNT_SNAPSHOT_SENDER}" \
    -D ASG_PREFIX_HTME="${ASG_PREFIX_HTME}" \
    -D ASG_PREFIX_HDI="${ASG_PREFIX_HDI}" \
    -D ASG_PREFIX_SNAPSHOT_SENDER="${ASG_PREFIX_SNAPSHOT_SENDER}" \
    -D ASG_MAX_COUNT_KAFKA_STUB="${ASG_MAX_COUNT_KAFKA_STUB}" \
    -D ASG_PREFIX_KAFKA_STUB="${ASG_PREFIX_KAFKA_STUB}" \
    -D ASG_MAX_COUNT_K2HB_MAIN_LONDON="${ASG_MAX_COUNT_K2HB_MAIN_LONDON}" \
    -D ASG_MAX_COUNT_K2HB_MAIN_DEDICATED_LONDON="${ASG_MAX_COUNT_K2HB_MAIN_DEDICATED_LONDON}" \
    -D ASG_MAX_COUNT_K2HB_EQUALITY_LONDON="${ASG_MAX_COUNT_K2HB_EQUALITY_LONDON}" \
    -D ASG_MAX_COUNT_K2HB_AUDIT_LONDON="${ASG_MAX_COUNT_K2HB_AUDIT_LONDON}" \
    -D ASG_PREFIX_K2HB_MAIN_LONDON="${ASG_PREFIX_K2HB_MAIN_LONDON}" \
    -D ASG_PREFIX_K2HB_MAIN_DEDICATED_LONDON="${ASG_PREFIX_K2HB_MAIN_DEDICATED_LONDON}" \
    -D ASG_PREFIX_K2HB_EQUALITY_LONDON="${ASG_PREFIX_K2HB_EQUALITY_LONDON}" \
    -D ASG_PREFIX_K2HB_AUDIT_LONDON="${ASG_PREFIX_K2HB_AUDIT_LONDON}" \
    -D ASG_PREFIX_INGESTION_ECS_CLUSTER="${ASG_PREFIX_INGESTION_ECS_CLUSTER}" \
    -D ASG_MAX_COUNT_INGESTION_ECS_CLUSTER="${ASG_MAX_COUNT_INGESTION_ECS_CLUSTER}" \
    -D AWS_DATASETS_BUCKET="${AWS_DATASETS_BUCKET}" \
    -D AWS_PUBLISHED_BUCKET="${AWS_PUBLISHED_BUCKET}" \
    -D AWS_DATA_INGRESS_STAGE_BUCKET="${AWS_DATA_INGRESS_STAGE_BUCKET}" \
    -D AWS_PROCESSED_BUCKET="${AWS_PROCESSED_BUCKET}"  \
    -D SYNTHETIC_RAWDATA_AWS_ACC="${SYNTHETIC_RAWDATA_AWS_ACC}" \
    -D SYNTHETIC_RAWDATA_PREFIX="${SYNTHETIC_RAWDATA_PREFIX}" \
    -D SYNTHETIC_ENCDATA_PREFIX="${SYNTHETIC_ENCDATA_PREFIX}" \
    -D AWS_DEFAULT_PROFILE="${AWS_DEFAULT_PROFILE}" \
    -D MANIFEST_REPORT_COUNT_OF_IDS="${MANIFEST_REPORT_COUNT_OF_IDS}" \
    -D IS_SYNTHETIC_DATA_INGESTION="${IS_SYNTHETIC_DATA_INGESTION}" \
    -D GENERATE_SNAPSHOTS_START_TIME_OVERRIDE="${GENERATE_SNAPSHOTS_START_TIME_OVERRIDE}" \
    -D GENERATE_SNAPSHOTS_END_TIME_OVERRIDE="${GENERATE_SNAPSHOTS_END_TIME_OVERRIDE}" \
    -D DYNAMO_DB_EXPORT_STATUS_TABLE_NAME="${DYNAMO_DB_EXPORT_STATUS_TABLE_NAME}" \
    -D DYNAMO_DB_PRODUCT_STATUS_TABLE_NAME="${DYNAMO_DB_PRODUCT_STATUS_TABLE_NAME}" \
    -D KAFKA_MESSAGE_VOLUME="${LOAD_TESTS_KAFKA_MESSAGE_VOLUME}" \
    -D KAFKA_RANDOM_KEY="${LOAD_TESTS_KAFKA_RANDOM_KEY}" \
    -D KAFKA_RECORD_COUNT="${LOAD_TESTS_KAFKA_RECORD_COUNT}" \
    -D HTME_DEFAULT_TOPIC_LIST_FULL_CONTENT="${HTME_DEFAULT_TOPIC_LIST_FULL_CONTENT}" \
    -D HTME_DEFAULT_TOPIC_LIST_INCREMENTALS_CONTENT="${HTME_DEFAULT_TOPIC_LIST_INCREMENTALS_CONTENT}" \
    -D HTME_DEFAULT_TOPIC_LIST_DRIFT_TESTING_INCREMENTALS_CONTENT="${HTME_DEFAULT_TOPIC_LIST_DRIFT_TESTING_INCREMENTALS_CONTENT}" \
    -D GENERATE_SNAPSHOTS_TRIGGER_SNAPSHOT_SENDER_OVERRIDE="${GENERATE_SNAPSHOTS_TRIGGER_SNAPSHOT_SENDER_OVERRIDE}" \
    -D GENERATE_SNAPSHOTS_TRIGGER_ADG_OVERRIDE="${GENERATE_SNAPSHOTS_TRIGGER_ADG_OVERRIDE}" \
    -D GENERATE_SNAPSHOTS_SEND_TO_RIS_OVERRIDE="${GENERATE_SNAPSHOTS_SEND_TO_RIS_OVERRIDE}" \
    -D GENERATE_SNAPSHOTS_CLEAR_S3_SNAPSHOTS="${GENERATE_SNAPSHOTS_CLEAR_S3_SNAPSHOTS}" \
    -D GENERATE_SNAPSHOTS_CLEAR_S3_MANIFESTS="${GENERATE_SNAPSHOTS_CLEAR_S3_MANIFESTS}" \
    -D GENERATE_SNAPSHOTS_REPROCESS_FILES_OVERRIDE="${GENERATE_SNAPSHOTS_REPROCESS_FILES_OVERRIDE}" \
    -D SNAPSHOT_S3_OUTPUT_PATH="${SNAPSHOT_S3_OUTPUT_PATH}" \
    -D SNAPSHOT_S3_OUTPUT_BUCKET="${SNAPSHOT_S3_OUTPUT_BUCKET}" \
    -D SNAPSHOT_S3_STATUS_PATH="${SNAPSHOT_S3_STATUS_PATH}" \
    -D MANIFEST_COMPARISON_TABLE_NAME_MISSING_IMPORTS_PARQUET="${MANIFEST_COMPARISON_TABLE_NAME_MISSING_IMPORTS_PARQUET}" \
    -D MANIFEST_COMPARISON_TABLE_NAME_MISSING_EXPORTS_PARQUET="${MANIFEST_COMPARISON_TABLE_NAME_MISSING_EXPORTS_PARQUET}" \
    -D MANIFEST_COMPARISON_DATABASE_NAME="${MANIFEST_COMPARISON_DATABASE_NAME}" \
    -D MANIFEST_COMPARISON_TABLE_NAME_COUNTS_PARQUET="${MANIFEST_COMPARISON_TABLE_NAME_COUNTS_PARQUET}" \
    -D MANIFEST_COMPARISON_TABLE_NAME_MISMATCHED_TIMESTAMPS_PARQUET="${MANIFEST_COMPARISON_TABLE_NAME_MISMATCHED_TIMESTAMPS_PARQUET}" \
    -D HISTORIC_IMPORTER_USE_ONE_MESSAGE_PER_PATH="${HISTORIC_IMPORTER_USE_ONE_MESSAGE_PER_PATH}" \
    -D HISTORIC_DATA_INGESTION_SKIP_EARLIER_THAN_OVERRIDE="${HISTORIC_DATA_INGESTION_SKIP_EARLIER_THAN_OVERRIDE}" \
    -D HISTORIC_DATA_INGESTION_SKIP_LATER_THAN_OVERRIDE="${HISTORIC_DATA_INGESTION_SKIP_LATER_THAN_OVERRIDE}" \
    -D HISTORIC_DATA_INGESTION_SKIP_EXISTING_RECORDS_OVERRIDE="${HISTORIC_DATA_INGESTION_SKIP_EXISTING_RECORDS_OVERRIDE}" \
    -D CORPORATE_DATA_INGESTION_SKIP_EARLIER_THAN_OVERRIDE="${CORPORATE_DATA_INGESTION_SKIP_EARLIER_THAN_OVERRIDE}" \
    -D CORPORATE_DATA_INGESTION_SKIP_LATER_THAN_OVERRIDE="${CORPORATE_DATA_INGESTION_SKIP_LATER_THAN_OVERRIDE}" \
    -D CORPORATE_DATA_INGESTION_PARTITIONS_COUNT_OVERRIDE="${CORPORATE_DATA_INGESTION_PARTITIONS_COUNT_OVERRIDE}" \
    -D CORPORATE_DATA_INGESTION_PREFIX_PER_EXECUTION_OVERRIDE="${CORPORATE_DATA_INGESTION_PREFIX_PER_EXECUTION_OVERRIDE}" \
    -D CORPORATE_DATA_INGESTION_USE_SPLIT_INPUTS_OVERRIDE="${CORPORATE_DATA_INGESTION_USE_SPLIT_INPUTS_OVERRIDE}" \
    -D HISTORIC_IMPORTER_CORRELATION_ID_OVERRIDE="${HISTORIC_IMPORTER_CORRELATION_ID_OVERRIDE}" \
    -D SNAPSHOT_SENDER_SCALE_UP_OVERRIDE="${SNAPSHOT_SENDER_SCALE_UP_OVERRIDE}" \
    -D INGEST_HBASE_EMR_CLUSTER_SNAPSHOT_TABLES_OVERRIDE="${INGEST_HBASE_EMR_CLUSTER_SNAPSHOT_TABLES_OVERRIDE}" \
    -D INGEST_HBASE_EMR_CLUSTER_HBCK_ARGUMENTS="${INGEST_HBASE_EMR_CLUSTER_HBCK_ARGUMENTS}" \
    -D INGEST_HBASE_EMR_CLUSTER_EMRFS_ARGUMENTS="${INGEST_HBASE_EMR_CLUSTER_EMRFS_ARGUMENTS}" \
    -D INGEST_HBASE_EMR_CLUSTER_EMRFS_PREFIX_OVERRIDE="${INGEST_HBASE_EMR_CLUSTER_EMRFS_PREFIX_OVERRIDE}" \
    -D INGEST_HBASE_EMR_CLUSTER_ID="${INGEST_HBASE_EMR_CLUSTER_ID}" \
    -D INGEST_HBASE_EMR_CLUSTER_ROOT_S3_BUCKET_ID="${INGEST_HBASE_EMR_CLUSTER_ROOT_S3_BUCKET_ID}" \
    -D INGEST_HBASE_EMR_CLUSTER_ROOT_S3_ROOT_DIRECTORY="${INGEST_HBASE_EMR_CLUSTER_ROOT_S3_ROOT_DIRECTORY}" \
    -D MONITORING_SNS_TOPIC_ARN="${MONITORING_SNS_TOPIC_ARN}" \
    -D AWS_ROLE_ARN="${AWS_ROLE_ARN}" \
    -D AWS_SESSION_TIMEOUT_SECONDS="${AWS_SESSION_TIMEOUT_SECONDS}" \
    -D K2HB_MAIN_MANIFEST_WRITE_S3_PREFIX="${K2HB_MAIN_MANIFEST_WRITE_S3_PREFIX}" \
    -D K2HB_EQUALITY_MANIFEST_WRITE_S3_PREFIX="${K2HB_EQUALITY_MANIFEST_WRITE_S3_PREFIX}" \
    -D K2HB_AUDIT_MANIFEST_WRITE_S3_PREFIX="${K2HB_AUDIT_MANIFEST_WRITE_S3_PREFIX}" \
    -D METADATA_STORE_TABLE_NAME_UCFS="${METADATA_STORE_TABLE_NAME_UCFS}" \
    -D METADATA_STORE_TABLE_NAME_EQUALITIES="${METADATA_STORE_TABLE_NAME_EQUALITIES}" \
    -D METADATA_STORE_TABLE_NAME_AUDIT="${METADATA_STORE_TABLE_NAME_AUDIT}" \
    -D RECONCILER_MAIN_CLUSTER_NAME="${RECONCILER_MAIN_CLUSTER_NAME}" \
    -D RECONCILER_EQUALITIES_CLUSTER_NAME="${RECONCILER_EQUALITIES_CLUSTER_NAME}" \
    -D RECONCILER_AUDIT_CLUSTER_NAME="${RECONCILER_AUDIT_CLUSTER_NAME}" \
    -D RECONCILER_MAIN_SERVICE_NAME="${RECONCILER_MAIN_SERVICE_NAME}" \
    -D RECONCILER_EQUALITIES_SERVICE_NAME="${RECONCILER_EQUALITIES_SERVICE_NAME}" \
    -D RECONCILER_AUDIT_SERVICE_NAME="${RECONCILER_AUDIT_SERVICE_NAME}" \
    -D RECONCILER_MAIN_DESIRED_TASK_COUNT="${RECONCILER_MAIN_DESIRED_TASK_COUNT}" \
    -D RECONCILER_EQUALITIES_DESIRED_TASK_COUNT="${RECONCILER_EQUALITIES_DESIRED_TASK_COUNT}" \
    -D RECONCILER_AUDIT_DESIRED_TASK_COUNT="${RECONCILER_AUDIT_DESIRED_TASK_COUNT}" \
    -D CDL_RUN_SCRIPT_S3_URL="${CDL_RUN_SCRIPT_S3_URL}" \
    -D CDL_SPLIT_INPUTS_S3_URL="${CDL_SPLIT_INPUTS_S3_URL}" \
    -D HDL_RUN_SCRIPT_S3_URL="${HDL_RUN_SCRIPT_S3_URL}" \
    -D CREATE_HBASE_TABLES_SCRIPT_S3_URL="${CREATE_HBASE_TABLES_SCRIPT_S3_URL}" \
    -D DATA_LOAD_TOPICS="${DATA_LOAD_TOPICS}" \
    -D DATA_LOAD_S3_SUFFIX="${DATA_LOAD_S3_SUFFIX}" \
    -D DATA_LOAD_METADATA_STORE_TABLE="${DATA_LOAD_METADATA_STORE_TABLE}" \
    -D DATA_LOAD_S3_FILE_PATTERN="${DATA_LOAD_S3_FILE_PATTERN}" \
    -D DATA_LOAD_S3_BASE_PREFIX="${DATA_LOAD_S3_BASE_PREFIX}" \
    -D CDL_DATA_LOAD_FILE_PATTERN_UCFS="${CDL_DATA_LOAD_FILE_PATTERN_UCFS}" \
    -D HDL_DATA_LOAD_S3_BASE_PREFIX="${HDL_DATA_LOAD_S3_BASE_PREFIX}" \
    -D CDL_DATA_LOAD_S3_BASE_PREFIX="${CDL_DATA_LOAD_S3_BASE_PREFIX}" \
    -D CORPORATE_STORAGE_S3_BUCKET_ID="${CORPORATE_STORAGE_S3_BUCKET_ID}" \
    -D DATA_STREAMING_TESTS_SKIP_RECONCILING="${DATA_STREAMING_TESTS_SKIP_RECONCILING}" \
    -D UCFS_CLAIMANT_DOMAIN_NAME="${UCFS_CLAIMANT_DOMAIN_NAME}" \
    -D UCFS_CLAIMANT_API_ACTIVE_REGION="${UCFS_CLAIMANT_API_ACTIVE_REGION}" \
    -D UCFS_CLAIMANT_API_PATH_V1_GET_AWARD_DETAILS="${UCFS_CLAIMANT_API_PATH_V1_GET_AWARD_DETAILS}" \
    -D UCFS_CLAIMANT_API_PATH_V2_GET_AWARD_DETAILS="${UCFS_CLAIMANT_API_PATH_V2_GET_AWARD_DETAILS}" \
    -D UCFS_CLAIMANT_API_SALT_SSM_PARAMETER_NAME="${UCFS_CLAIMANT_API_SALT_SSM_PARAMETER_NAME}" \
    -D UCFS_CLAIMANT_API_KAFKA_CONSUMER_CLUSTER_NAME="${UCFS_CLAIMANT_API_KAFKA_CONSUMER_CLUSTER_NAME}" \
    -D UCFS_CLAIMANT_API_KAFKA_CONSUMER_SERVICE_NAME="${UCFS_CLAIMANT_API_KAFKA_CONSUMER_SERVICE_NAME}" \
    -D UCFS_CLAIMANT_API_KAFKA_CONSUMER_SERVICE_DESIRED_TASK_COUNT="${UCFS_CLAIMANT_API_KAFKA_CONSUMER_SERVICE_DESIRED_TASK_COUNT}" \
    -D DATAWORKS_MODEL_OUTPUT_BUCKET="${DATAWORKS_MODEL_OUTPUT_BUCKET}" \
    -D DATAWORKS_STREAMS_KAFKA_PRODUCER="${DATAWORKS_STREAMS_KAFKA_PRODUCER}" \
    -D DATAWORKS_STREAMS_KAFKA_PRODUCER_HSM_KEY_ID="${DATAWORKS_STREAMS_KAFKA_PRODUCER_HSM_KEY_ID}" \
    -D DATAWORKS_STREAMS_KAFKA_PRODUCER_HSM_PUB_KEY="${DATAWORKS_STREAMS_KAFKA_PRODUCER_HSM_PUB_KEY}" \
    -D DATAWORKS_MODEL_OUTPUT_SQS="${DATAWORKS_MODEL_OUTPUT_SQS}" \
    -D DATAWORKS_STREAMS_KAFKA_DLQ_CONSUMER="${DATAWORKS_STREAMS_KAFKA_DLQ_CONSUMER}" \
    -D DATAWORKS_DLQ_OUTPUT_BUCKET="${DATAWORKS_DLQ_OUTPUT_BUCKET}" \
    -D AWS_REGION_MAIN="${AWS_REGION_MAIN}" \
    -D AWS_REGION_ALTERNATIVE="${AWS_REGION_ALTERNATIVE}" \
    -D PDM_DATA_CLASSIFICATION_CSV_KEY="${PDM_DATA_CLASSIFICATION_CSV_KEY}" \
    -D UC_FEATURE_DATA_CLASSIFICATION="${UC_FEATURE_DATA_CLASSIFICATION}" \
    -D PDM_DATA_CLASSIFICATION="${PDM_DATA_CLASSIFICATION}" \
    -D DATAWORKS_COMMON_CONFIG_BUCKET="${DATAWORKS_COMMON_CONFIG_BUCKET}"

    export test_exit_code=$?

    set +x
}

local_folder="/tmp/e2e_config/"
get_secret_values

download_config "${CONFIG_LOCATION}" "${local_folder}"
set_file_locations "${local_folder}"

echo "Inputs: META_FOLDER=${META_FOLDER}"
echo "Inputs: TF_INGEST_OUTPUT_FILE=${TF_INGEST_OUTPUT_FILE}"
echo "Inputs: TF_INTERNAL_COMPUTE_OUTPUT_FILE=${TF_INTERNAL_COMPUTE_OUTPUT_FILE}"
echo "Inputs: TF_SNAPSHOT_SENDER_OUTPUT_FILE=${TF_SNAPSHOT_SENDER_OUTPUT_FILE}"
echo "Inputs: TF_MGMT_OUTPUT_FILE=${TF_MGMT_OUTPUT_FILE}"
echo "Inputs: TF_ADG_OUTPUT_FILE=${TF_ADG_OUTPUT_FILE}"
echo "Inputs: TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE=${TF_DATAWORKS_AWS_INGEST_CONSUMERS_FILE}"
echo "Inputs: TF_COMMON_OUTPUT_FILE=${TF_COMMON_OUTPUT_FILE}"
echo "Inputs: TF_UCFS_CLAIMANT_OUTPUT_FILE=${TF_UCFS_CLAIMANT_OUTPUT_FILE}"
echo "Inputs: TF_DATAWORKS_AWS_UCFS_CLAIMANT_CONSUMER=${TF_DATAWORKS_AWS_UCFS_CLAIMANT_CONSUMER}"
echo "Inputs: TF_COMMON_OUTPUT_FILE=${TF_COMMON_OUTPUT_FILE}"
echo "Inputs: TF_DATAWORKS_AWS_INGESTION_ECS_CLUSTER=${TF_DATAWORKS_AWS_INGESTION_ECS_CLUSTER}"
echo "Inputs: TF_DATAWORKS_STREAMS_KAFKA_PRODUCER_APP=${TF_DATAWORKS_STREAMS_KAFKA_PRODUCER_APP}"
echo "Inputs: TF_DATAWORKS_STREAMS_KAFKA_CONSUMER_APP=${TF_DATAWORKS_STREAMS_KAFKA_CONSUMER_APP}"
echo "Inputs: TF_DATAWORKS_AWS_S3_OBJECT_TAGGER=${TF_DATAWORKS_AWS_S3_OBJECT_TAGGER}"
echo "Inputs: TF_DATAWORKS_HBASE_EXPORT=${TF_DATAWORKS_HBASE_EXPORT}"
echo "Inputs: TF_DATAWORKS_HBASE_IMPORT=${TF_DATAWORKS_HBASE_IMPORT}"

execute_behave

delete_config "${local_folder}"

exit $test_exit_code
