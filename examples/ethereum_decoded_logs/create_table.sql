attach table if not exists {{.CHAIN}}_decoded_logs uuid '{{.TABLE_UUID}}' (
    timestamp DateTime64(3, 'UTC') CODEC(ZSTD),
    block_hash FixedString(32) CODEC(ZSTD),
    block_number UInt64 CODEC(ZSTD),
    transaction_from FixedString(20) CODEC(ZSTD),
    transaction_status UInt8 CODEC(ZSTD),
    transaction_hash FixedString(32) CODEC(ZSTD),
    transaction_index UInt32 CODEC(ZSTD),
    log_index UInt32 CODEC(ZSTD),
    address FixedString(20) CODEC(ZSTD),
    signature String CODEC(ZSTD),
    inputs JSON CODEC(ZSTD),

    index idx_timestamp timestamp type minmax granularity 1,
    index idx_block_hash block_hash type bloom_filter granularity 4,
    index idx_transaction_from transaction_from type bloom_filter granularity 4,
    index idx_transaction_hash transaction_hash type bloom_filter granularity 4,
    index idx_address address type bloom_filter granularity 4,
    index idx_signature signature type bloom_filter granularity 4
)
engine = ReplacingMergeTree
partition by toYYYYMM(timestamp)
order by (
    address,
    signature,
    timestamp,
    block_number,
    log_index
)
settings 
    disk = disk(
        type=s3,
        endpoint='{{.S3_ENDPOINT}}',
        region='{{.S3_REGION}}',
        access_key_id='{{.S3_ACCESS_KEY_ID}}',
        secret_access_key='{{.S3_SECRET_ACCESS_KEY}}',
        use_environment_credentials=0,
        metadata_type=plain_rewritable,
        readonly=false
    ),
    min_bytes_for_wide_part=4294967296,
    min_rows_for_wide_part=1000000000