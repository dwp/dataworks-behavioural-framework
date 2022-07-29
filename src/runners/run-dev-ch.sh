#!/bin/bash

test_run_id="${1:-NOT_SET}"
test_run_type="dataworks-aws-ch"
feature_tag="@dataworks-aws-ch"
mongo_import_key="NOT_SET"
number_of_topics=3

if [[ "${test_run_id}" == "NOT_SET" ]]; then
  echo "You must specify test_run_id"
  exit 1
fi

./src/runners/run-dev-by-tags.sh "${test_run_id}" "${test_run_type}" "${feature_tag}" "${mongo_import_key}" "${number_of_topics}"
