attach table if not exists ethereum_logs uuid '61248174-7a4c-4abf-9974-a6b745d93ce8' (
    timestamp DateTime64(3, 'UTC') CODEC(ZSTD),
    block_hash FixedString(32) CODEC(ZSTD),
    block_number UInt64 CODEC(ZSTD),
    transaction_from FixedString(20) CODEC(ZSTD),
    transaction_status UInt8 CODEC(ZSTD),
    transaction_hash FixedString(32) CODEC(ZSTD),
    transaction_index UInt32 CODEC(ZSTD),
    removed Bool CODEC(ZSTD),
    log_index UInt32 CODEC(ZSTD),
    address FixedString(20) CODEC(ZSTD),
    data String CODEC(ZSTD),
    topics Array(FixedString(32)) CODEC(ZSTD),

    index idx_timestamp timestamp type minmax granularity 1,
    index idx_block_hash block_hash type bloom_filter granularity 4,
    index idx_transaction_from transaction_from type bloom_filter granularity 4,
    index idx_transaction_hash transaction_hash type bloom_filter granularity 4,
    index idx_address address type bloom_filter granularity 4,
    index idx_topics_1 topics[1] type bloom_filter granularity 4
)
engine = ReplacingMergeTree
partition by toYYYYMM(timestamp)
order by (block_number, log_index)
settings 
    disk = disk(
        type=s3,
        endpoint='{{.S3_ENDPOINT}}',
        region='{{.S3_REGION}}',
        use_environment_credentials=1,
        metadata_type=plain_rewritable,
        readonly=false
    ),
    min_bytes_for_wide_part=536870912