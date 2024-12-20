create temporary table buffer_{{.START}}_{{.END}}
as (
    with
        {{.START}} as start,
        {{.END}} as end,

        block_numbers as (
            select 
                generate_series as n 
            from generate_series(start, end)
        ),

        deps as (
            select 
                n,
                tuple(
                    toDateTime64(evm_hex_decode_int(b.timestamp, 'Int64'), 3, 'UTC') as timestamp,
                    evm_hex_decode(b.hash) as hash,
                    evm_hex_decode_int(b.number, 'UInt64') as number
                ) as block
            from (
                select
                    n,
                    JSONExtract(
                        ethereum_rpc(
                            'eth_getBlockByNumber', 
                            [evm_hex_encode_int(n), 'false'], 
                            '{{.RPC_ENDPOINT}}#fail-on-error=true&fail-on-null=true'
                        ),
                        'value',
                        'Tuple(
                            timestamp String,
                            number String,
                            hash String,
                        )'
                    ) as b
                from block_numbers
            )
        ),

        logs as (
            select
                n,
                evm_hex_decode(r.from) as transaction_from,
                evm_hex_decode_int(r.status, 'UInt8') as transaction_status,
                evm_hex_decode(r.transactionHash) as transaction_hash,
                evm_hex_decode_int(r.transactionIndex, 'UInt32') as transaction_index,
                toBool(l.removed) as removed,
                evm_hex_decode_int(l.logIndex, 'UInt32') as log_index,
                evm_hex_decode(l.address) as address,
                evm_hex_decode(l.data) as data,
                arrayMap(x -> evm_hex_decode(x), l.topics) as topics
            from block_numbers 
            array join JSONExtract(
                ethereum_rpc(
                    'eth_getBlockReceipts', 
                    [evm_hex_encode_int(n)], 
                    '{{.RPC_ENDPOINT}}#fail-on-error=true&fail-on-null=true'
                ),
                'value',
                'Array(
                    Tuple(
                        from String,
                        status String,
                        transactionHash String,
                        transactionIndex String,
                        logs Array(
                            Tuple(
                                removed String,
                                logIndex String,
                                address String,
                                data String,
                                topics Array(String)
                            )
                        )
                    )
                )'
            ) as r
            array join r.logs as l
        )

    select
        d.block.timestamp as timestamp,
        d.block.hash as block_hash,
        d.block.number as block_number,
        l.transaction_from as transaction_from,
        l.transaction_status as transaction_status,
        l.transaction_hash as transaction_hash,
        l.transaction_index as transaction_index,
        l.removed as removed,
        l.log_index as log_index,
        l.address as address,
        l.data as data,
        l.topics as topics
    from logs as l
    left join deps as d on l.n = d.n
)