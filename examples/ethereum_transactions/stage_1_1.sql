create temporary table {{.CHAIN}}_transactions_{{.START}}_{{.END}}
as (
    with
        {{.START}} as start,
        {{.END}} as end,

        block_numbers as (
            select 
                generate_series as n 
            from generate_series(start, end)
        ),

        rows as (
            select
                JSONExtract(
                    ethereum_rpc(
                        'eth_getBlockByNumber', 
                        [evm_hex_encode_int(n), 'true'], 
                        '{{.RPC_ENDPOINT}}#fail-on-error=true&fail-on-null=true'
                    ),
                    'value',
                    'Tuple(
                        timestamp String,
                        number String,
                        hash String,
                        transactions Array(
                            Tuple(
                                accessList Array(
                                    Tuple(
                                        address String,
                                        storageKeys Array(String)
                                    )
                                ),
                                chainId String,
                                from String,
                                gas String,
                                gasPrice String,
                                hash String,
                                input String,
                                maxFeePerGas String,
                                maxPriorityFeePerGas String,
                                nonce String,
                                r String,
                                s String,
                                to String,
                                transactionIndex String,
                                type String,
                                v String,
                                value String,
                                yParity String

                                {{ if .ENABLE_DENCUN }},
                                maxFeePerBlobGas String,
                                blobVersionedHashes Array(String)
                                {{ end }}

                                {{ if .ENABLE_OP_STACK }},
                                sourceHash String,
                                mint String,
                                isSystemTx String
                                {{ end }}
                            )
                        )
                    )'
                ) as block,
                JSONExtract(
                    ethereum_rpc(
                        'eth_getBlockReceipts', 
                        [evm_hex_encode_int(n)], 
                        '{{.RPC_ENDPOINT}}#fail-on-error=true&fail-on-null=true'
                    ),
                    'value',
                    'Array(
                        Tuple(  
                            contractAddress String,
                            cumulativeGasUsed String,
                            effectiveGasPrice String,
                            gasUsed String,
                            root String,
                            status String
                            
                            {{ if .ENABLE_DENCUN }},
                            blobGasUsed String,
                            blobGasPrice String
                            {{ end }}

                            {{ if .ENABLE_OP_STACK }},
                            depositNonce String,
                            depositReceiptVersion String,
                            l1GasPrice String,
                            l1GasUsed String,
                            l1Fee String,
                            l1FeeScalar String,
                            l1BlobBaseFee String,
                            l1BaseFeeScalar String,
                            l1BlobBaseFeeScalar String
                            {{ end }}
                        )
                    )'
                ) as receipts
            from block_numbers
        )

    select 
        toDateTime64(evm_hex_decode_int(block.timestamp, 'Int64'), 3, 'UTC') as timestamp,
        arrayMap(
            x -> tuple(
                evm_hex_decode(x.address),
                arrayMap(x -> evm_hex_decode(x), x.storageKeys)
            ),   
            tx.accessList
        ) as access_list,
        evm_hex_decode(block.hash) as block_hash,
        evm_hex_decode_int(block.number, 'UInt64') as block_number,
        evm_hex_decode_int(tx.chainId, 'UInt32') as chain_id,
        evm_hex_decode(tx.from) as from,
        evm_hex_decode_int(tx.gas, 'UInt64') as gas,
        evm_hex_decode_int(tx.gasPrice, 'UInt256') as gas_price,
        evm_hex_decode(tx.hash) as hash,
        evm_hex_decode(tx.input) as input,
        evm_hex_decode_int(tx.maxFeePerGas, 'UInt256') as max_fee_per_gas,
        evm_hex_decode_int(tx.maxPriorityFeePerGas, 'UInt256') as max_priority_fee_per_gas,
        evm_hex_decode_int(tx.nonce, 'UInt256') as nonce,
        evm_hex_decode(tx.r) as r,
        evm_hex_decode(tx.s) as s,
        evm_hex_decode(tx.to) as to,
        evm_hex_decode_int(tx.transactionIndex, 'UInt32') as transaction_index,
        evm_hex_decode_int(tx.type, 'UInt16') as type,
        evm_hex_decode(tx.v) as v,
        evm_hex_decode_int(tx.value, 'UInt256') as value,
        evm_hex_decode_int(tx.yParity, 'UInt8') as yParity,
        evm_hex_decode(r.contractAddress) as contract_address,
        evm_hex_decode_int(r.cumulativeGasUsed, 'UInt64') as cumulative_gas_used,
        evm_hex_decode_int(r.effectiveGasPrice, 'UInt256') as effective_gas_price,
        evm_hex_decode_int(r.gasUsed, 'UInt64') as gas_used,
        evm_hex_decode(r.root) as root,
        evm_hex_decode_int(r.status, 'UInt8') as status

        {{ if .ENABLE_DENCUN }},
        evm_hex_decode_int(tx.maxFeePerBlobGas, 'UInt256') as max_fee_per_blob_gas,
        arrayMap(x -> evm_hex_decode(x), tx.blobVersionedHashes) as blob_versioned_hashes,
        evm_hex_decode_int(r.blobGasUsed, 'UInt64') as blob_gas_used,
        evm_hex_decode_int(r.blobGasPrice, 'UInt256') as blob_gas_price
        {{ end }}

        {{ if .ENABLE_OP_STACK }},
        evm_hex_decode(tx.sourceHash) as source_hash,
        evm_hex_decode_int(tx.mint, 'UInt256') as mint,
        toBool(if(length(tx.isSystemTx) > 0, tx.isSystemTx::String, 'false')) as is_system_tx,
        evm_hex_decode_int(r.depositNonce, 'UInt256') as deposit_nonce,
        evm_hex_decode_int(r.depositReceiptVersion, 'UInt64') as deposit_receipt_version,
        evm_hex_decode_int(r.l1GasPrice, 'UInt256') as l1_gas_price,
        evm_hex_decode_int(r.l1GasUsed, 'UInt64') as l1_gas_used,
        evm_hex_decode_int(r.l1Fee, 'UInt256') as l1_fee,
        evm_hex_decode_int(r.l1FeeScalar, 'UInt64') as l1_fee_scalar,
        evm_hex_decode_int(r.l1BlobBaseFee, 'UInt256') as l1_blob_base_fee,
        evm_hex_decode_int(r.l1BaseFeeScalar, 'UInt64') as l1_base_fee_scalar,
        evm_hex_decode_int(r.l1BlobBaseFeeScalar, 'UInt64') as l1_blob_base_fee_scalar
        {{ end }}
    from rows
    array join block.transactions as tx, receipts as r
)




