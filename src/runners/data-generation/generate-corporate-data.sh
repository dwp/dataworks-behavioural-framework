#!/bin/bash

test_run_id="${1:-NOT_SET}"
record_count="${2:-10}"
key_method="${3:-different}"
number_of_topics="${4:-5}"

test_run_type="generate-corporate-data"
feature_tag="@generate-corporate-data"

if [[ "${test_run_id}" == "NOT_SET" ]]; then
  echo "Defaulting test_run_id to 1"
  test_run_id=1
fi

export DATA_GENERATION_RECORD_COUNT="${record_count}"
export DATA_GENERATION_METHOD="${key_method}"

sh ../run-dev-by-tags.sh "${test_run_id}" "${test_run_type}" "${feature_tag}" "NOT_SET" "${number_of_topics}"
