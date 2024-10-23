create temporary table {{.CHAIN}}_logs_transformed_{{.START}}_{{.END}} 
as select * from (
    with
        q0 as (
            select
                JSONExtract(block, 'JSON') as block,
                JSONExtract(receipts, 'Array(JSON)') as receipts
            from {{.CHAIN}}_logs_extracted_{{.START}}_{{.END}}
        )

        select
            toDateTime64(evm_hex_decode_int(block.timestamp::String, 'Int64'), 3, 'UTC') as timestamp,
            evm_hex_decode(receipt.blockHash::String) as block_hash,
            evm_hex_decode_int(receipt.blockNumber::String, 'UInt64') as block_number,
            evm_hex_decode(receipt.from::String) as transaction_from,
            evm_hex_decode_int(receipt.status::String, 'UInt8') as transaction_status,
            evm_hex_decode(receipt.transactionHash::String) as transaction_hash,
            evm_hex_decode_int(receipt.transactionIndex::String, 'UInt32') as transaction_index,
            toBool(log.removed::String) as removed,
            evm_hex_decode_int(log.logIndex::String, 'UInt32') as log_index,
            evm_hex_decode(log.address::String) as address,
            evm_hex_decode(log.data::String) as data,
            arrayMap(x -> evm_hex_decode(x), log.topics::Array(String)) as topics
        from q0
        array join receipts as receipt
        array join receipt.logs[] as log
)
settings max_execution_time = 300