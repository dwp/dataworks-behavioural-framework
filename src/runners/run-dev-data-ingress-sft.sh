#!/bin/bash

test_run_id="${1:-NOT_SET}"
test_run_type="data-ingress"
feature_tag="@data-ingress-sft"

if [[ "${test_run_id}" == "NOT_SET" ]]; then
  echo "Defaulting test_run_id to 1"
  test_run_id=1
fi

./src/runners/run-dev-by-tags.sh "${test_run_id}" "${test_run_type}" "${feature_tag}"
