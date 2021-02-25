select 'Total missing imports', count(*) as id_count from [parquet_table_name_missing_imports]
union all
select 'Total missing imports of type insert', count(*) as id_count from [parquet_table_name_missing_imports]
where lower(export_type) = 'mongo_insert'
union all
select 'Total missing imports of type update', count(*) as id_count from [parquet_table_name_missing_imports]
where lower(export_type) = 'mongo_update'
union all
select 'Total missing imports of type delete', count(*) as id_count from [parquet_table_name_missing_imports]
where lower(export_type) = 'mongo_delete'
union all
select 'Total missing imports of type import', count(*) as id_count from [parquet_table_name_missing_imports]
where lower(export_type) = 'mongo_import'
union all
select 'Total missing imports of type other', count(*) as id_count from [parquet_table_name_missing_imports]
where lower(export_type) <> 'mongo_insert'
and lower(export_type) <> 'mongo_update'
and lower(export_type) <> 'mongo_delete'
and lower(export_type) <> 'mongo_import'
union all
select 'Total missing exports', count(*) as id_count from [parquet_table_name_missing_exports]
union all
select 'Total missing exports of type insert', count(*) as id_count from [parquet_table_name_missing_exports]
where lower(import_type) = 'mongo_insert'
union all
select 'Total missing exports of type update', count(*) as id_count from [parquet_table_name_missing_exports]
where lower(import_type) = 'mongo_update'
union all
select 'Total missing exports of type delete', count(*) as id_count from [parquet_table_name_missing_exports]
where lower(import_type) = 'mongo_delete'
union all
select 'Total missing exports of type import', count(*) as id_count from [parquet_table_name_missing_exports]
where lower(import_type) = 'mongo_import'
union all
select 'Total missing exports of type other', count(*) as id_count from [parquet_table_name_missing_exports]
where lower(import_type) <> 'mongo_insert'
and lower(import_type) <> 'mongo_update'
and lower(import_type) <> 'mongo_delete'
and lower(import_type) <> 'mongo_import'
order by 1, 2;
