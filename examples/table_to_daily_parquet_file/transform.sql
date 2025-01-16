insert into 
table function s3(
    '{{.TARGET_ENDPOINT}}/date={_partition_id}/data.parquet', 
    '{{.S3_ACCESS_KEY_ID}}',
    '{{.S3_SECRET_ACCESS_KEY}}',
    'Parquet'
)
partition by toDate(timestamp)
select
    *
from {{.SOURCE_TABLE}}
where timestamp >= addDays(toDate(0), {{.START}}) and timestamp < addDays(toDate(1), {{.END}})