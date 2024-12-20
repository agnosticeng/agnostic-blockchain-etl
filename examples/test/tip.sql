select
    evm_hex_decode_int(
        JSONExtract(
            ethereum_rpc(
                'eth_getBlockByNumber', 
                ['"latest"', 'false'], 
                'https://eth.llamarpc.com#fail-on-error=true&fail-on-null=true'
            ), 
            'value',
            'number', 
            'String'
        ), 
        'UInt64'
    ) as tip
