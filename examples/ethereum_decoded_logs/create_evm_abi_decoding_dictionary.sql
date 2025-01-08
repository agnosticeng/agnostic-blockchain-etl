create dictionary evm_abi_decoding (
    selector String,
    fullsigs Array(String)
)
primary key selector
source(http(url '{{.EVM_ABI_DECODING_URL}}' format 'Parquet'))
lifetime(min 3600 max 7200)
layout(hashed())


