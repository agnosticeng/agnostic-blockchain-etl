create temporary table {{.CHAIN}}_transactions_extracted_{{.START}}_{{.END}} 
as select * from (
    with
        block_numbers as (
            select 
                generate_series as n 
            from generate_series(
                {{.START}}::UInt64,
                {{.END}}::UInt64
            )
        )

    select
        n as block_number,
        ethereum_rpc(
            'eth_getBlockByNumber', 
            [evm_hex_encode_int(n), 'true'], 
            '{{.RPC_ENDPOINT}}'
        ) as block,
        ethereum_rpc(
            'eth_getBlockReceipts', 
            [evm_hex_encode_int(n)], 
            '{{.RPC_ENDPOINT}}'
        ) as receipts
    from block_numbers
)
settings max_execution_time = 300