#!/bin/bash

test_run_id="${1:-NOT_SET}"
snapshot_type="${2:-NOT_SET}"
test_run_type="dataload"
mongo_import_key="NOT_SET"
feature_tags="@admin-send-$snapshot_type-snapshots-to-crown"

if [[ "${test_run_id}" == "NOT_SET" ]]; then
  echo "Defaulting test_run_id to 1"
  test_run_id=1
fi

if [[ "${snapshot_type}" == "NOT_SET" ]]; then
  echo "You must specify a snapshot_type i.e. 'full' or 'incremental'"
  exit 1
fi

.././run-dev-by-tags.sh "${test_run_id}" "${test_run_type}" "${feature_tags}" "${mongo_import_key}"
