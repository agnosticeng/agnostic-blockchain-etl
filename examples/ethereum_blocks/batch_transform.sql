create temporary table ethereum_blocks_transformed_{{.START_BLOCK}}_{{.END_BLOCK}} 
as select * from (
        with
            q0 as (
                select 
                    JSONExtract(block, 'JSON') as block,
                from ethereum_blocks_extracted_{{.START_BLOCK}}_{{.END_BLOCK}}
            )

        select 
            toDateTime64(evm_hex_decode_int(block.timestamp::String, 'Int64'), 3, 'UTC') as timestamp,
            evm_hex_decode_int(block.baseFeePerGas::String, 'UInt256') as base_fee_per_gas,
            evm_hex_decode_int(block.blobGasUsed::String, 'UInt64') as blob_gas_used,
            evm_hex_decode_int(block.difficulty::String, 'UInt256') as difficulty,
            evm_hex_decode_int(block.excessBlobGas::String, 'UInt64') as excess_blob_gas,
            evm_hex_decode(block.extraData::String) as extra_data,
            evm_hex_decode_int(block.gasLimit::String, 'UInt64') as gas_limit,
            evm_hex_decode_int(block.gas_used::String, 'UInt64') as gas_used,
            evm_hex_decode(block.hash::String) as hash,
            evm_hex_decode(block.logsBloom::String) as logs_bloom,
            evm_hex_decode(block.miner::String) as miner,
            evm_hex_decode(block.mix_hash::String) as mix_hash,
            evm_hex_decode_int(block.nonce::String, 'UInt256') as nonce,
            evm_hex_decode_int(block.number::String, 'UInt64') as number,
            evm_hex_decode(block.parentBeaconBlockRoot::String) as parent_beacon_block_root,
            evm_hex_decode(block.parentHash::String) as parent_hash,
            evm_hex_decode(block.receiptsRoot::String) as receipts_root,
            evm_hex_decode(block.sha3Uncles::String) as sha3_uncles,
            evm_hex_decode_int(block.size::String, 'UInt32') as size,
            evm_hex_decode(block.stateRoot::String) as state_root,
            evm_hex_decode_int(block.totalDifficulty::String, 'UInt256') as total_difficulty,
            evm_hex_decode(block.transactionsRoot::String) as transactions_root,
            arrayMap(x -> evm_hex_decode(x), block.uncles::Array(String)) as uncles,
            evm_hex_decode(block.withdrawalsRoot::String) as withdrawals_root
        from q0
)
settings max_execution_time = 300