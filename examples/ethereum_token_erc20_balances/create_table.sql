attach table if not exists {{.CHAIN}}_token_erc20_balances uuid '{{.TABLE_UUID}}' (
    timestamp DateTime64(3, 'UTC') CODEC(ZSTD),
    block_hash FixedString(32) CODEC(ZSTD),
    block_number UInt64 CODEC(ZSTD),
    wallet_address FixedString(20) CODEC(ZSTD),
    token_address FixedString(20) CODEC(ZSTD),
    token_symbol String CODEC(ZSTD),
    token_decimals UInt8 CODEC(ZSTD),
    raw_balance UInt256 CODEC(ZSTD),
    balance Float64 CODEC(ZSTD),

    index idx_timestamp timestamp type minmax granularity 1,
    index idx_block_number block_number type minmax granularity 1,
    index idx_block_hash block_hash type bloom_filter granularity 4,
    index idx_token_address token_address type bloom_filter granularity 2
)
engine = ReplacingMergeTree
partition by toYYYYMM(timestamp)
order by (
    wallet_address,
    token_address,
    timestamp,
    block_number
)
settings 
    disk = disk(
        type=cache,
        max_size='{{.CACHE_MAX_SIZE}}',
        path='{{.CACHE_PATH}}',
        disk = disk(
            type=s3,
            endpoint='{{.S3_ENDPOINT}}',
            region='{{.S3_REGION}}',
            access_key_id='{{.S3_ACCESS_KEY_ID}}',
            secret_access_key='{{.S3_SECRET_ACCESS_KEY}}',
            use_environment_credentials=0,
            metadata_type=plain_rewritable,
            readonly=false
        )
    ),
    min_bytes_for_wide_part=4294967296,
    min_rows_for_wide_part=1000000000