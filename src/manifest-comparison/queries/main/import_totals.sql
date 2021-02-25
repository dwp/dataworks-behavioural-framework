select 
    count_if(imported_count > 0) as imported_collections_count,
    sum(imported_count) as imported_count
from [parquet_table_name_counts]
order by 1, 2;
