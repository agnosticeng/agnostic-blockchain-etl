create temporary table {{.CHAIN}}_traces_transformed_{{.START}}_{{.END}} 
as select * from (
    with
        q0 as (
            select
                JSONExtract(block, 'Tuple(timestamp String)') as block,
                JSONExtract(receipts, 'Array(JSON)') as receipts,
                JSONExtract(traces, 'Array(JSON)') as traces
            from {{.CHAIN}}_traces_extracted_{{.START}}_{{.END}}
        ),

        q1 as (
            select 
                block,
                trace,
                receipts[trace.transactionPosition::UInt32+1] as receipt
            from q0
            array join traces as trace
        )

    select 
        toDateTime64(evm_hex_decode_int(block.timestamp, 'Int64'), 3, 'UTC') as timestamp,
        evm_hex_decode(receipt.blockHash::String) as block_hash,
        evm_hex_decode_int(receipt.blockNumber::String, 'UInt64') as block_number,
        evm_hex_decode(receipt.from::String) as transaction_from,
        evm_hex_decode_int(receipt.status::String, 'UInt8') as transaction_status,
        evm_hex_decode(receipt.transactionHash::String) as transaction_hash,
        evm_hex_decode_int(receipt.transactionIndex::String, 'UInt32') as transaction_index,
        trace.subtraces::UInt32 as subtraces,
        trace.traceAddress::Array(UInt32) as trace_address,
        trace.type::String as type,
        trace.error::String as error,
        trace.action.callType::String as call_type,
        evm_hex_decode(trace.action.from::String) as from,
        evm_hex_decode_int(trace.action.gas::String, 'UInt64') as gas,
        evm_hex_decode(trace.action.input::String) as input,
        evm_hex_decode(trace.action.to::String) as to,  
        evm_hex_decode_int(trace.action.value::String, 'UInt256') as value,
        evm_hex_decode(trace.action.address::String) as address,  
        evm_hex_decode_int(trace.action.balance::String, 'UInt256') as balance,
        evm_hex_decode(trace.action.refundAddress::String) as refund_address,  
        evm_hex_decode(trace.action.author::String) as author,  
        trace.action.rewardType::String as reward_type,
        evm_hex_decode(trace.action.init::String) as init,
        evm_hex_decode(trace.result.address::String) as result_address,
        evm_hex_decode(trace.result.code::String) as result_code,
        evm_hex_decode_int(trace.result.gasUsed::String, 'UInt64') as gas_used,
        evm_hex_decode(trace.result.output::String) as output
    from q1
)
settings max_execution_time = 300