#!/bin/bash

test_run_id="${1:-NOT_SET}"
snapshot_type="${2:-NOT_SET}"
test_run_type="dataload"
mongo_import_key="NOT_SET"
feature_tags="@admin-generate-${snapshot_type}-snapshots"

if [[ "${test_run_id}" == "NOT_SET" ]]; then
  echo "You must specify a test_run_id i.e. '123'"
  exit 1
fi

if [[ "${snapshot_type}" == "NOT_SET" ]]; then
  echo "You must specify a snapshot_type of either 'full' or 'incremental'"
  exit 1
fi

.././run-dev-by-tags.sh "${test_run_id}" "${test_run_type}" "${feature_tags}" "${mongo_import_key}"
