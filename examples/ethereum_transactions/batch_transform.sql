create temporary table {{.CHAIN}}_transactions_transformed_{{.START_BLOCK}}_{{.END_BLOCK}} 
as select * from (
    with 
        q0 as (
            select 
                JSONExtract(block, 'JSON') as block,
                JSONExtract(receipts, 'Array(JSON)') as receipts
            from {{.CHAIN}}_transactions_extracted_{{.START_BLOCK}}_{{.END_BLOCK}}
        )

    select 
        toDateTime64(evm_hex_decode_int(block.timestamp::String, 'Int64'), 3, 'UTC') as timestamp,
        arrayMap(x -> tuple(
                evm_hex_decode(JSONExtract(x, 'address', 'String')),
                arrayMap(x -> evm_hex_decode(x), JSONExtract(x, 'storageKeys', 'Array(String)'))
            ),   
            tx.accessList::Array(String)
        ) as access_list,
        evm_hex_decode(tx.blockHash::String) as block_hash,
        evm_hex_decode_int(tx.blockNumber::String, 'UInt64') as block_number,
        evm_hex_decode_int(tx.chainId::String, 'UInt32') as chain_id,
        evm_hex_decode(tx.from::String) as from,
        evm_hex_decode_int(tx.gas::String, 'UInt64') as gas,
        evm_hex_decode_int(tx.gasPrice::String, 'UInt256') as gas_price,
        evm_hex_decode(tx.hash::String) as hash,
        evm_hex_decode(tx.input::String) as input,
        evm_hex_decode_int(tx.maxFeePerGas::String, 'UInt256') as max_fee_per_gas,
        evm_hex_decode_int(tx.maxPriorityFeePerGas::String, 'UInt256') as max_priority_fee_per_gas,
        evm_hex_decode_int(tx.nonce::String, 'UInt256') as nonce,
        evm_hex_decode(tx.r::String) as r,
        evm_hex_decode(tx.s::String) as s,
        evm_hex_decode(tx.to::String) as to,
        evm_hex_decode_int(tx.transactionIndex::String, 'UInt32') as transaction_index,
        evm_hex_decode_int(tx.type::String, 'UInt16') as type,
        evm_hex_decode(tx.v::String) as v,
        evm_hex_decode_int(tx.value::String, 'UInt256') as value,
        evm_hex_decode_int(tx.yParity::String, 'UInt8') as yParity,
        evm_hex_decode(receipt.contractAddress::String) as contract_address,
        evm_hex_decode_int(receipt.cumulativeGasUsed::String, 'UInt64') as cumulative_gas_used,
        evm_hex_decode_int(receipt.effectiveGasPrice::String, 'UInt256') as effective_gas_price,
        evm_hex_decode_int(receipt.gasUsed::String, 'UInt64') as gas_used,
        evm_hex_decode(receipt.root::String) as root,
        evm_hex_decode_int(receipt.status::String, 'UInt8') as status

        {{ if .ENABLE_DENCUN }},
        evm_hex_decode_int(tx.maxFeePerBlobGas::String, 'UInt256') as max_fee_per_blob_gas,
        arrayMap(x -> evm_hex_decode(x), tx.blobVersionedHashes::Array(String)) as blob_versioned_hashes,
        evm_hex_decode_int(receipt.blobGasUsed::String, 'UInt64') as blob_gas_used,
        evm_hex_decode_int(receipt.blobGasPrice::String, 'UInt256') as blob_gas_price
        {{ end }}

        {{ if .ENABLE_OP_STACK }},
        evm_hex_decode(tx.sourceHash::String) as source_hash,
        evm_hex_decode_int(tx.mint::String, 'UInt256') as mint,
        toBool(tx.isSystemTx::String) as is_system_tx,
        evm_hex_decode_int(receipt.depositNonce::String, 'UInt256') as deposit_nonce,
        evm_hex_decode_int(receipt.depositReceiptVersion::String, 'UInt64') as deposit_receipt_version,
        evm_hex_decode_int(receipt.l1GasPrice::String, 'UInt256') as l1_gas_price,
        evm_hex_decode_int(receipt.l1GasUsed::String, 'UInt64') as l1_gas_used,
        evm_hex_decode_int(receipt.l1Fee::String, 'UInt256') as l1_fee,
        evm_hex_decode_int(receipt.l1FeeScalar::String, 'UInt64') as l1_fee_scalar,
        evm_hex_decode_int(receipt.l1BlobBaseFee::String, 'UInt256') as l1_blob_base_fee,
        evm_hex_decode_int(receipt.l1BaseFeeScalar::String, 'UInt64') as l1_base_fee_scalar,
        evm_hex_decode_int(receipt.l1BlobBaseFeeScalar::String, 'UInt64') as l1_blob_base_fee_scalar
        {{ end }}
    from q0
    array join block.transactions[] as tx, receipts as receipt
)
settings max_execution_time = 300



