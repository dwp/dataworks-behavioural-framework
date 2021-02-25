select 
    sum(missing_imported_count) as missing_imported_count
from [parquet_table_name_counts]
where
    missing_imported_count > 0 and
    exported_count > 0 and
    missing_imported_count <> exported_count;
