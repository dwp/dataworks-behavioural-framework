select 
    database,
    collection,
    exported_count,
    missing_imported_count,
    cast(cast(missing_imported_count as decimal(18,2)) / cast(exported_count as decimal(18,2)) * 100 as integer) as percentage_missing
from [parquet_table_name_counts]
where 
    missing_imported_count > 0 and
    exported_count > 0 and
    missing_imported_count <> exported_count
order by 4 desc, 1, 2;
