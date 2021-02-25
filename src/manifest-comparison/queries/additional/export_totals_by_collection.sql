select 
    database,
    collection,
    exported_count,
    exported_count_from_data_load,
    exported_count_from_streaming_feed,
    latest_export_timestamp,
    earliest_export_timestamp
from [parquet_table_name_counts]
where exported_count > 0
order by 1, 2;
