insert into ethereum_blocks
select * from ethereum_blocks_transformed_{{.START_BLOCK}}_{{.END_BLOCK}}