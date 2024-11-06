attach table if not exists {{.CHAIN}}_traces uuid '{{.UUID}}' (
    timestamp DateTime64(3, 'UTC') CODEC(ZSTD),
    block_hash FixedString(32) CODEC(ZSTD),
    block_number UInt64 CODEC(ZSTD),
    transaction_from FixedString(20) CODEC(ZSTD),
    transaction_status UInt8 CODEC(ZSTD),
    transaction_hash FixedString(32) CODEC(ZSTD),
    transaction_index UInt32 CODEC(ZSTD),
    subtraces UInt32 CODEC(ZSTD),
    trace_address Array(UInt32) CODEC(ZSTD),
    type LowCardinality(String) CODEC(ZSTD),
    error String CODEC(ZSTD),
    call_type LowCardinality(String) CODEC(ZSTD),
    from FixedString(20) CODEC(ZSTD),
    gas UInt64 CODEC(ZSTD),
    input String CODEC(ZSTD),
    to FixedString(20) CODEC(ZSTD),
    value UInt256 CODEC(ZSTD),
    address FixedString(20) CODEC(ZSTD),
    balance UInt256 CODEC(ZSTD),
    refund_address FixedString(20) CODEC(ZSTD),
    author FixedString(20) CODEC(ZSTD),
    reward_type String CODEC(ZSTD),
    init String CODEC(ZSTD),
    result_address FixedString(20) CODEC(ZSTD),
    result_code String CODEC(ZSTD),
    gas_used UInt64 CODEC(ZSTD),
    output String CODEC(ZSTD),

    index idx_timestamp timestamp type minmax granularity 1,
    index idx_block_hash block_hash type bloom_filter granularity 4,
    index idx_transaction_from transaction_from type bloom_filter granularity 4,
    index idx_transaction_hash transaction_hash type bloom_filter granularity 4,
    index idx_from from type bloom_filter granularity 4,
    index idx_to to type bloom_filter granularity 4,
    index idx_input_four_bytes left(input, 4) type bloom_filter granularity 4,
    index idx_result_address result_address type bloom_filter granularity 4
)
engine = ReplacingMergeTree
partition by toYYYYMM(timestamp)
order by (block_number, trace_address)
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