#!/bin/bash

test_run_id="${1:-NOT_SET}"
test_run_type="dataload"
mongo_import_key="${2:-NOT_SET}"
feature_tags="@admin-historic-data-import"

if [[ "${test_run_id}" == "NOT_SET" ]]; then
  echo "Defaulting test_run_id to 1"
  test_run_id=1
fi

if [[ "${mongo_import_key}" == "NOT_SET" ]]; then
  echo "You must specify a mongo_import_key for use in the MONGO_SNAPSHOT_S3_PREFIXES i.e. an old importer run like markmatthews_importer_dev_667"
  exit 1
fi

.././run-dev-by-tags.sh "${test_run_id}" "${test_run_type}" "${feature_tags}" "${mongo_import_key}"
