select database, collection, id, export_component, export_type, earliest_timestamp, latest_timestamp, earliest_manifest
from (
    select database, collection, id, export_component, export_type, earliest_timestamp, latest_timestamp, earliest_manifest, row_number() over (
        partition by database, collection
        order by id desc) as rn
    from [parquet_table_name_mismatched]
    where lower(earliest_manifest) = 'export'
)
where rn <= [count_of_ids]
order by 1, 2, 3;
