select 
    database,
    collection,
    imported_count,
    latest_import_timestamp,
    earliest_import_timestamp
from [parquet_table_name_counts]
where imported_count > 0
order by 1, 2;
