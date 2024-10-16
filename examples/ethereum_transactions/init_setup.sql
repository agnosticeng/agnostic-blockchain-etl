attach table if not exists ethereum_transactions uuid '3d3e7c5c-7d34-4cba-bef9-1cf2e5a45a2e' (
    timestamp DateTime64(3, 'UTC') CODEC(ZSTD),
    access_list Array(
        Tuple(
            address FixedString(20), 
            storage_keys Array(FixedString(32))
        )
    ),
    block_hash FixedString(32) CODEC(ZSTD),
    block_number UInt64 CODEC(ZSTD),
    chain_id UInt32 CODEC(ZSTD),
    from FixedString(20) CODEC(ZSTD),
    gas UInt64 CODEC(ZSTD),
    gas_price UInt256 CODEC(ZSTD),
    hash FixedString(32) CODEC(ZSTD),
    input String CODEC(ZSTD),
    max_fee_per_gas UInt256 CODEC(ZSTD),
    max_priority_fee_per_gas UInt256 CODEC(ZSTD),
    nonce UInt256 CODEC(ZSTD),
    r FixedString(32) CODEC(ZSTD),
    s FixedString(32) CODEC(ZSTD),
    to FixedString(20) CODEC(ZSTD),
    transaction_index UInt32 CODEC(ZSTD),
    type UInt16 CODEC(ZSTD),
    v String CODEC(ZSTD),
    value UInt256 CODEC(ZSTD),
    y_parity UInt8 CODEC(ZSTD),
    contract_address FixedString(20) CODEC(ZSTD),
    cumulative_gas_used UInt64 CODEC(ZSTD),
    effective_gas_price UInt256 CODEC(ZSTD),
    gas_used UInt64 CODEC(ZSTD),
    root FixedString(32) CODEC(ZSTD),
    status UInt8 CODEC(ZSTD),

    index idx_timestamp timestamp type minmax granularity 1,
    index idx_block_hash block_hash type bloom_filter granularity 4,
    index idx_from from type bloom_filter granularity 4,
    index idx_hash hash type bloom_filter granularity 4,
)
engine = ReplacingMergeTree
partition by toYYYYMM(timestamp)
order by (block_number, transaction_index)
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