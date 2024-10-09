insert into ethereum_transactions 
select * from ethereum_transactions_transformed_{{.START_BLOCK}}_{{.END_BLOCK}}