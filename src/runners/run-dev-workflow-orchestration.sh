#!/bin/bash

test_run_id="${1:-NOT_SET}"
test_run_type="workflow-orchestration-poc"
feature_tag="@workflow-orchestration-poc"

if [[ "${test_run_id}" == "NOT_SET" ]]; then
  echo "Defaulting test_run_id to 1"
  test_run_id=1
fi

./run-dev-by-tags.sh "${test_run_id}" "${test_run_type}" "${feature_tag}"
