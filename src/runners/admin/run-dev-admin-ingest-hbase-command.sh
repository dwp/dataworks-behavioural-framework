#!/bin/bash

test_run_id="1"
hbase_command_type="${1:-NOT_SET}"
arguments="${2}"

if [[ "${hbase_command_type}" == "NOT_SET" ]]; then
  echo "You must specify a hbase_command_type i.e. 'emrfs-sync'"
  exit 1
fi

test_run_type="admin-ingest-hbase-${hbase_command_type}"
mongo_import_key="NOT_SET"
feature_tags="@admin-ingest-hbase-${hbase_command_type}"

.././run-dev-by-tags.sh "${test_run_id}" "${test_run_type}" "${feature_tags}" "${mongo_import_key}" 3 600 1 "false" 10 "${arguments}"
