#!/bin/bash

test_run_id="${1:-NOT_SET}"
test_run_type="load-test-kafka"
feature_tag="@load-test-kafka"
mongo_import_key="NOT_SET"
number_of_topics=3
e2e_test_timeout=9000
load_tests_kafka_message_volume="10"
load_tests_kafka_random_key="true"
load_tests_kafka_record_count="10"

if [[ "${test_run_id}" == "NOT_SET" ]]; then
  echo "Defaulting test_run_id to 1"
  test_run_id=1
fi

./run-dev-by-tags.sh "${test_run_id}" "${test_run_type}" "${feature_tag}" "${mongo_import_key}" "${number_of_topics}" "${e2e_test_timeout}" "${load_tests_kafka_message_volume}" "${load_tests_kafka_random_key}" "${load_tests_kafka_record_count}"
