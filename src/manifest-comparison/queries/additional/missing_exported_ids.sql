select 
    database, 
    collection, 
    id, 
    import_timestamp,
    import_component,
    import_type
from (
    select database, collection, id, import_timestamp, import_component, import_type, row_number() over (
        partition by database, collection
        order by id desc) as rn
    from [parquet_table_name_missing_exports]
)
where rn <= [count_of_ids]
order by 1, 2, 3;
