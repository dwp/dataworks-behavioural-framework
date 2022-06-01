#!/bin/bash

test_run_id="${1:-NOT_SET}"
test_run_type="hbase-import"
feature_tag="@hbase-import-flow"
mongo_import_key="NOT_SET"
number_of_topics=1

if [[ "${test_run_id}" == "NOT_SET" ]]; then
  echo "Defaulting test_run_id to 1"
  test_run_id=1
fi

./run-dev-by-tags.sh "${test_run_id}" "${test_run_type}" "${feature_tag}" "${mongo_import_key}" "${number_of_topics}"
