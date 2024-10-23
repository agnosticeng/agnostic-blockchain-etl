select
    evm_hex_decode_int(
        JSONExtract(
            ethereum_rpc('eth_getBlockByNumber', ['"{{.LATEST_BLOCK_STATUS}}"', 'false'], '{{.RPC_ENDPOINT}}'), 
            'number', 
            'String'
        ), 
        'UInt64'
    ) as max_end
