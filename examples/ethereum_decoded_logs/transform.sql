insert into sink
select * from (
    with
        q0 as (
            select
                timestamp,
                block_hash,
                block_number,
                transaction_from,
                transaction_status,
                transaction_hash,
                transaction_index,
                log_index,
                address,
                topics,
                data
            from source
            where block_number >= {{.START}} and block_number <= {{.END}}
            and length(topics) > 0
        ),

        q1 as (
            select 
                q0.* except (topics, data),
                JSONExtract(
                    evm_decode_event(
                        topics::Array(String),
                        data::String,
                        dictGet(evm_abi_decoding, 'fullsigs', topics[1]::String)
                    ),
                    'JSON'
                ) as evt
            from q0
        )


    select
        * except (evt),
        evt.value.signature::String as signature,
        evt.^value.inputs as inputs
    from q1
    where evt.error is null
)

----

-- create dictionary evm_abi_decoding (selector String, fullsigs Array(String)) primary key selector source(http(url 'https://pub-c95b23ccaa6b4a92a8a1411feca96564.r2.dev/sourcify/evm_abi_decoding.parquet' format 'Parquet')) lifetime(min 3600 max 7200) layout(hashed())

-- select * from evm_abi_decoding limit 10

-- create table source as remote('clickhouse-ch-open.infra:9000', 'default', 'ethereum_mainnet_logs')

-- with
--     q0 as (
--         select
--             timestamp,
--             block_hash,
--             block_number,
--             transaction_from,
--             transaction_status,
--             transaction_hash,
--             transaction_index,
--             log_index,
--             address,
--             topics,
--             data
--         from source
--         where block_number >= {{.START}} and block_number <= {{.END}}
--         and length(topics) > 0
--     ),

--     q1 as (
--         select 
--             q0.* except (topics, data),
--             JSONExtract(
--                 evm_decode_event(
--                     topics::Array(String),
--                     data::String,
--                     dictGet(evm_abi_decoding, 'fullsigs', topics[1]::String)
--                 ),
--                 'JSON'
--             ) as evt
--         from q0
--     )


-- select
--     * except (evt),
--     evt.value.signature::String as signature,
--     evt.^value.inputs as inputs
-- from q1
-- where evt.error is null