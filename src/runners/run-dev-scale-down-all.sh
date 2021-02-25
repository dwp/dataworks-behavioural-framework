#!/bin/bash

test_run_id="${1:-NOT_SET}"
test_run_type="scaler"
mongo_import_key="NOT_SET"

if [[ "${test_run_id}" == "NOT_SET" ]]; then
  echo "Defaulting test_run_id to 1"
  test_run_id=1
fi

./run-dev-by-tags.sh "${test_run_id}" "${test_run_type}" "@admin-scale-down-hdi" "${mongo_import_key}"
./run-dev-by-tags.sh "${test_run_id}" "${test_run_type}" "@admin-scale-down-htme" "${mongo_import_key}"
./run-dev-by-tags.sh "${test_run_id}" "${test_run_type}" "@admin-scale-down-snapshotsender" "${mongo_import_key}"
