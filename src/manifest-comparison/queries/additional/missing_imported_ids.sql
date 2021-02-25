select 
    database, 
    collection, 
    id, 
    export_timestamp,
    export_component,
    export_type
from (
    select database, collection, id, export_timestamp, export_component, export_type, row_number() over (
        partition by database, collection
        order by id desc) as rn
    from [parquet_table_name_missing_imports]
)
where rn <= [count_of_ids]
order by 1, 2, 3;
