insert into 
table function s3(
    '{{.TARGET_ENDPOINT}}/{_partition_id}.parquet', 
    '{{.S3_ACCESS_KEY_ID}}',
    "{{.S3_SECRET_ACCESS_KEY}}",
    'Parquet'
)
partition by toYYYYMMDD(timestamp)
select
    *
from {{.SOURCE_TABLE}}
where timestamp >= addDays(toDate(0), {{.START}}) and timestamp < addDays(toDate(1), {{.END}})
