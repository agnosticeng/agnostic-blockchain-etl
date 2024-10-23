select
    maxOrDefault(toUInt64(toDate(replaceOne(_file, '.parquet', '')))) + 1 as start
from s3('{{.TARGET_ENDPOINT}}/*.parquet', 'One')