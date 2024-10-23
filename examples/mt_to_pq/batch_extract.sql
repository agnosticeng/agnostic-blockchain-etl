insert into 
table function s3('{{.TARGET_ENDPOINT}}/{_partition_id}.parquet', 'Parquet')
partition by toYYYYMMDD(timestamp)
select
    *
from {{.SOURCE_TABLE}} 
where timestamp >= addDays(toDate(0), {{.START}}) and timestamp < addDays(toDate(0), {{.END}})
settings input_format_parquet_use_native_reader=1

