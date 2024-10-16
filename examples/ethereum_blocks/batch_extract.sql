create temporary table ethereum_blocks_extracted_{{.START_BLOCK}}_{{.END_BLOCK}} 
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
        ethereum_rpc(
            'eth_getBlockByNumber', 
            [evm_hex_encode_int(n), 'false'], 
            ''
        ) as block
    from block_numbers
)
settings max_execution_time = 300