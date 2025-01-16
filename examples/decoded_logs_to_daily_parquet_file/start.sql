with 
    (
        select
            count(*)
        from s3(
            '{{.TARGET_ENDPOINT}}/date=*/*.parquet', 
            '{{.S3_ACCESS_KEY_ID}}',
            '{{.S3_SECRET_ACCESS_KEY}}',
            'One'
        )
    ) as num_files

select if(
    num_files = 0, 
    null,
    (
        select
            toUInt64(max(date)) + 1
        from s3(
            '{{.TARGET_ENDPOINT}}/date=*/*.parquet', 
            '{{.S3_ACCESS_KEY_ID}}',
            '{{.S3_SECRET_ACCESS_KEY}}',
            'One'
        )
    )
) as start