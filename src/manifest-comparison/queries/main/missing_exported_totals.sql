select 
    sum(missing_exported_count) as missing_exported_count
from [parquet_table_name_counts]
where
    missing_exported_count > 0 and
    imported_count > 0 and
    missing_exported_count <> imported_count;
