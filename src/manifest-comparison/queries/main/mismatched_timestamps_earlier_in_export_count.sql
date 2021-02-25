select 
    database,
    collection,
    exported_count,
    mismatched_timestamps_earlier_in_export_count,
    cast(cast(mismatched_timestamps_earlier_in_export_count as decimal(18,2)) / cast(exported_count as decimal(18,2)) * 100 as integer) as percentage_mismatched
from [parquet_table_name_counts]
where mismatched_timestamps_earlier_in_export_count > 0
order by 1, 2;
