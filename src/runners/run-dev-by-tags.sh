#!/bin/sh
cd "$(dirname "$0")"

test_run_id="${1:-NOT_SET}"
test_run_type="${2:-NOT_SET}"
feature_tag="${3:-NOT_SET}"
mongo_import_key="${4:-NOT_SET}"
number_of_topics_to_use="${5:-3}"
e2e_test_timeout="${6:-600}"
load_tests_kafka_message_volume="${7:1}"
load_tests_kafka_random_key="${8:false}"
load_tests_kafka_record_count="${9:10}"
arguments="${10}"
emrfs_prefix_override="${11}"

function set_overrides() {
    echo "Setting overrides for the local test run"

    export AWS_PROFILE_FOR_DOWNLOADS="dataworks-development"
    export IS_SYNTHETIC_DATA_INGESTION=""
    export OVERRIDE_ROLE="true"

    if [[ "${feature_tag}" == "@admin-ingest-hbase-hbck" ]]; then
    export INGEST_HBASE_EMR_CLUSTER_HBCK_ARGUMENTS="${arguments}"
    echo "Set the hbck arguments as '${ingest_hbase_emr_cluster_hbck_arguments}'"
    elif [[ "${feature_tag}" == "@admin-ingest-hbase-snapshot-tables" ]]; then
    export INGEST_HBASE_EMR_CLUSTER_SNAPSHOT_TABLES_OVERRIDE="${arguments}"
    echo "Set the snapshot tables override as '${ingest_hbase_emr_cluster_snapshot_table_override}'"
    elif [[ "${feature_tag}" == "@admin-ingest-hbase-emrfs-import" ]] || [[ hbase_command_type == "@admin-ingest-hbase-emrfs-sync" ]] || [[ hbase_command_type == "@admin-ingest-hbase-emrfs-delete" ]]; then
    export INGEST_HBASE_EMR_CLUSTER_EMRFS_ARGUMENTS="${arguments}"
    echo "Set the emrfs arguments as '${ingest_hbase_emr_cluster_emrfs_arguments}'"
    export INGEST_HBASE_EMR_CLUSTER_EMRFS_PREFIX_OVERRIDE="${emrfs_prefix_override}"
    echo "Set the emrfs sync root override as '${INGEST_HBASE_EMR_CLUSTER_EMRFS_PREFIX_OVERRIDE}'"
    fi
}

function set_meta() {
    echo "Setting meta for the local test run"

    meta_location="${1}"

    echo "Clear or make local meta directory"
    if [[ ! -d "${meta_location}" ]]; then
        mkdir "${meta_location}"
    else
        rm -rf "${meta_location}"
    fi

    echo "${USER}" > $meta_location/build_pipeline_name
    echo "${test_run_type}" > $meta_location/build_job_name
    echo "${test_run_id}" > $meta_location/build_name
}

function delete_meta() {
    echo "Deleting local temporary meta data"

    meta_location="${1}"
    rm -rf "${meta_location}"
}

function run_tests() {
    echo "Executing tests locally against development environment"
    
    meta_location="${1}"
    export E2E_FEATURE_TAG_FILTER="${feature_tag}"

    ./run-ci.sh "${meta_location}" "--profile ${AWS_PROFILE_FOR_DOWNLOADS}"
   export test_exit_code=$?
}

if [[ "${test_run_id}" == "NOT_SET" ]]; then
  echo "You must specify a test_run_id i.e. '123'"
  exit 1
fi

if [[ "${test_run_type}" == "NOT_SET" ]]; then
  echo "You must specify a test_run_type i.e. 'end-to-end | data-streaming | data-egress | data-load'"
  exit 1
fi

if [[ "${feature_tag}" == "NOT_SET" ]]; then
  echo "You must specify feature_tag i.e. '--tags=@data-streaming | --tags=@data-import | --tags=@data-export | --tags=@data-load'"
  exit 1
fi

echo "Inputs: test_run_id=${test_run_id}"
echo "Inputs: test_run_type=${test_run_type}"
echo "Inputs: feature_tag=${feature_tag}"
echo "Inputs: mongo_import_key=${mongo_import_key}"
echo "Inputs: number_of_topics_to_use=${number_of_topics_to_use}"
echo "Inputs: e2e_test_timeout=${e2e_test_timeout}"
echo "Inputs: load_tests_kafka_message_volume=${load_tests_kafka_message_volume}"
echo "Inputs: load_tests_kafka_random_key=${load_tests_kafka_random_key}"
echo "Inputs: load_tests_kafka_record_count=${load_tests_kafka_record_count}"
echo "Inputs: arguments=${arguments}"
echo "Inputs: emrfs_prefix_override=${emrfs_prefix_override}"

meta_folder_name="/tmp/e2e_meta"

set_overrides
set_meta "${meta_folder_name}"
run_tests "${meta_folder_name}"
delete_meta "${meta_folder_name}"

exit $test_exit_code
