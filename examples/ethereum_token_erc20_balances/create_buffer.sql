create temporary table buffer_{{.START}}_{{.END}}
as (
    with 
        logs as (
            select
                timestamp,
                block_hash,
                block_number,
                address,
                JSONExtract(
                    evm_decode_event(
                        topics::Array(FixedString(32)),
                        data::String,
                        ['event Transfer(address indexed,address indexed,uint256)']
                    ),
                    'JSON'
                ) as evt
            from source
            where block_number >= {{.START}} and block_number <= {{.END}}
            and topics[1] = keccak256('Transfer(address,address,uint256)')
            and length(topics) == 3
            having evt.error is null
        ),

        transfers as (
            select
                timestamp,
                block_hash,
                block_number,
                address as token_address,
                evt.value.inputs.arg0::String as sender,
                evt.value.inputs.arg1::String as recipient,
                evt.value.inputs.arg2::UInt256 as amount
            from logs
        ),

        tokens as (
            select 
                token_address,
                JSONExtractString(
                    ethereum_rpc_call(
                        evm_hex_encode(token_address), 
                        'function symbol()(string)', 
                        '', 
                        -1::Int64, 
                        '{{.RPC_ENDPOINT}}#fail-on-retryable-error=true&fail-on-null=true'
                    ),
                    'value',
                    'arg0'
                ) as symbol,
                JSONExtractUInt(
                    ethereum_rpc_call(
                        evm_hex_encode(token_address), 
                        'function decimals()(uint8)', 
                        '',  
                        -1::Int64, 
                        '{{.RPC_ENDPOINT}}#fail-on-retryable-error=true&fail-on-null=true'
                    ),
                    'value',
                    'arg0'
                ) as decimals
            from transfers
            group by token_address
        ),

        grouped as (
            select  
                any(timestamp) as timestamp,
                any(block_hash) as block_hash,
                block_number,
                wallet_address,
                token_address    
            from (
                (
                    select 
                        * except (sender, recipient),
                        sender as wallet_address
                    from transfers
                )
                union all 
                (
                    select 
                        * except (sender, recipient),
                        recipient as wallet_address 
                    from transfers
                )   
            )
            group by wallet_address, token_address, block_number
        ),

        balances as (
            select 
                grouped.*,
                JSONExtract(
                    ethereum_rpc_call(
                        evm_hex_encode(token_address), 
                        'function balanceOf(address)(uint256)',
                        toJSONString([wallet_address]),
                        block_number::Int64,
                        '{{.RPC_ENDPOINT}}#fail-on-retryable-error=true&fail-on-null=true'
                    ),
                    'value',
                    'arg0',
                    'UInt256'
                ) as balance
            from grouped   
        )

    select
        b.timestamp as timestamp,
        b.block_hash as block_hash,
        b.block_number as block_number,
        evm_hex_decode(b.wallet_address) as wallet_address,
        b.token_address as token_address,
        t.symbol as token_symbol,
        t.decimals as token_decimals,
        b.balance as raw_balance,
        (b.balance / exp10(t.decimals)) as balance
    from balances as b
    left join tokens as t on b.token_address = t.token_address
)