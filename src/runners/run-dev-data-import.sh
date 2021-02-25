#!/bin/bash

test_run_id="${1:-NOT_SET}"
test_run_type="historic-importer"
feature_tag="@data-import"
mongo_import_key="NOT_SET"
number_of_topics=3

if [[ "${test_run_id}" == "NOT_SET" ]]; then
  echo "Defaulting test_run_id to 1"
  test_run_id=1
fi

./run-dev-by-tags.sh "${test_run_id}" "${test_run_type}" "${feature_tag}" "${mongo_import_key}" "${number_of_topics}"
