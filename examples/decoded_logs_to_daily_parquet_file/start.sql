with 
    (
        select
            count(*) as num_files,
            max(date) as max_date
        from s3(
            '{{.TARGET_ENDPOINT}}/date=*/*.parquet', 
            '{{.S3_ACCESS_KEY_ID}}',
            '{{.S3_SECRET_ACCESS_KEY}}',
            'One'
        )
    ) as prev

select if(prev.num_files = 0, null, toUInt64(prev.max_date)+1) as start