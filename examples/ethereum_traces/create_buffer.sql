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
                    toDateTime64(evm_hex_decode_int(b.timestamp, 'Int64'), 3, 'UTC') as block_timestamp,
                    evm_hex_decode(b.hash) as block_hash,
                    evm_hex_decode_int(b.number, 'UInt64') as block_number
                ) as block,
                receipts
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
                    ) as b,
                    arrayMap(
                        r -> (
                            tuple(
                                evm_hex_decode(r.from) as transaction_from,
                                evm_hex_decode_int(r.status, 'UInt8') as transaction_status,
                                evm_hex_decode(r.transactionHash) as transaction_hash,
                                evm_hex_decode_int(r.transactionIndex, 'UInt32') as transaction_index
                            )
                        ), 
                        JSONExtract(
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
                                    transactionIndex String
                                )
                            )'
                        )
                    ) as receipts
                from block_numbers
            )
        ),

        traces as (
            select
                n,
                t.transactionPosition as transaction_position,
                t.subtraces as subtraces,
                t.traceAddress as trace_address,
                t.type as type,
                t.error as error,
                t.action.callType as call_type,
                evm_hex_decode(t.action.from::String) as from,
                evm_hex_decode_int(t.action.gas::String, 'UInt64') as gas,
                evm_hex_decode(t.action.input::String) as input,
                evm_hex_decode(t.action.to::String) as to,  
                evm_hex_decode_int(t.action.value::String, 'UInt256') as value,
                evm_hex_decode(t.action.address::String) as address,  
                evm_hex_decode_int(t.action.balance::String, 'UInt256') as balance,
                evm_hex_decode(t.action.refundAddress::String) as refund_address,  
                evm_hex_decode(t.action.author::String) as author,  
                t.action.rewardType::String as reward_type,
                evm_hex_decode(t.action.init::String) as init,
                evm_hex_decode(t.result.address::String) as result_address,
                evm_hex_decode(t.result.code::String) as result_code,
                evm_hex_decode_int(t.result.gasUsed::String, 'UInt64') as gas_used,
                evm_hex_decode(t.result.output::String) as output
            from block_numbers 
            array join JSONExtract(
                ethereum_rpc(
                    'trace_block', 
                    [evm_hex_encode_int(n)], 
                    '{{.RPC_ENDPOINT}}#fail-on-error=true&fail-on-null=true'
                ),
                'value',
                'Array(
                    Tuple(
                        transactionPosition UInt32,
                        subtraces UInt32,
                        traceAddress Array(UInt32),
                        type String,
                        error String,
                        action Tuple(
                            callType String,
                            from String,
                            gas String,
                            input String,
                            to String,
                            value String,
                            address String,
                            balance String,
                            refundAddress String,
                            author String,
                            rewardType String,
                            init String,
                        ),
                        result Tuple(
                            address String,
                            code String,
                            gasUsed String,
                            output String
                        )
                    )
                )'
            ) as t
        )

    select 
        d.block.block_timestamp as timestamp,
        d.block.block_hash as block_hash,
        d.block.block_number as block_number,
        d.receipts[t.transaction_position+1].transaction_from as transaction_from,
        d.receipts[t.transaction_position+1].transaction_status as transaction_status,
        d.receipts[t.transaction_position+1].transaction_hash as transaction_hash,
        d.receipts[t.transaction_position+1].transaction_index as transaction_index,
        t.subtraces as subtraces,
        t.trace_address as trace_address,
        t.type as type,
        t.error as error,
        t.call_type as call_type,
        t.from as from,
        t.gas as gas,
        t.input as input,
        t.to as to,
        t.value as value,
        t.address as address,
        t.balance as balance,
        t.refund_address as refund_address,
        t.author as author,
        t.reward_type as reward_type,
        t.init as init,
        t.result_address as result_address,
        t.result_code as result_code,
        t.gas_used as gas_used,
        t.output as output
    from traces as t
    left join deps as d on t.n = d.n
)