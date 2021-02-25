with imported_databases as 
(
    select 
        database
    from (
        select database,
            sum(imported_count) as total_imported_count,
            sum(exported_count) as total_exported_count
        from [parquet_table_name_counts]
        group by 1
    )
    where total_imported_count > 0
    order by 1
)
select distinct
    database,
    collection
from [parquet_table_name_counts]
where imported_count = 0 and exported_count > 0
and database = any (select database from imported_databases)
order by 1, 2;
