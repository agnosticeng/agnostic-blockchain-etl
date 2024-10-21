select
    evm_hex_decode_int(
        JSONExtract(
            ethereum_rpc('eth_getBlockByNumber', ['"safe"', 'false'], '{{.RPC_ENDPOINT}}'), 
            'number', 
            'String'
        ), 
        'UInt64'
    ) as max_end_block
