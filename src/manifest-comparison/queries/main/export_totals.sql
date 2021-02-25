select 
    count_if(exported_count > 0) as exported_collections_count,
    sum(exported_count) as exported_count,
    sum(exported_count_from_data_load) as exported_count_from_data_load,
    sum(exported_count_from_streaming_feed) as exported_count_from_streaming_feed
from [parquet_table_name_counts]
order by 1, 2;
