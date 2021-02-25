select 
    database
from (
    select database,
        sum(imported_count) as total_imported_count,
        sum(exported_count) as total_exported_count
    from [parquet_table_name_counts]
    group by 1
)
where total_exported_count = 0 and total_imported_count > 0
order by 1;
