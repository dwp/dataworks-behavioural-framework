select 
    database,
    collection,
    imported_count,
    missing_exported_count,
    cast(cast(missing_exported_count as decimal(18,2)) / cast(imported_count as decimal(18,2)) * 100 as integer) as percentage_missing
from [parquet_table_name_counts]
where
    missing_exported_count > 0 and
    imported_count > 0 and
    missing_exported_count <> imported_count
order by 4 desc, 1, 2;
