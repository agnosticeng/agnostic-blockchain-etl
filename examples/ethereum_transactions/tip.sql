select
    evm_hex_decode_int(
        JSONExtract(
            ethereum_rpc(
                'eth_getBlockByNumber', 
                ['"{{.LATEST_BLOCK_STATUS}}"', 'false'], 
                '{{.RPC_ENDPOINT}}#fail-on-error=true&fail-on-null=true'
            ), 
            'value',
            'number', 
            'String'
        ), 
        'UInt64'
    ) as tip
