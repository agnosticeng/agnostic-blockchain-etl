select
    toUInt64(maxOrDefault(toDate(replaceOne(_file, '.parquet', ''))) + 1) as start
from s3(
    '{{.TARGET_ENDPOINT}}/*.parquet', 
    '{{.S3_ACCESS_KEY_ID}}',
    "{{.S3_SECRET_ACCESS_KEY}}",
    'One'
)
settings remote_filesystem_read_prefetch=false