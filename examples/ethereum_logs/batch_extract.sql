create temporary table ethereum_logs_extracted_{{.START_BLOCK}}_{{.END_BLOCK}} 
as select * from (
    with
        block_numbers as (
            select 
                generate_series as n 
            from generate_series(
                {{.START_BLOCK}}::UInt64,
                {{.END_BLOCK}}::UInt64
            )
        )

    select
        n as block_number,
        ethereum_rpc(
            'eth_getBlockByNumber', 
            [evm_hex_encode_int(n), 'false'], 
            ''
        ) as block,
        ethereum_rpc(
            'eth_getBlockReceipts', 
            [evm_hex_encode_int(n)], 
            ''
        ) as receipts
    from block_numbers
)
settings max_execution_time = 300