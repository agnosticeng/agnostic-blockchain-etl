insert into {{.CHAIN}}_transactions 
select * from {{.CHAIN}}_transactions_transformed_{{.START_BLOCK}}_{{.END_BLOCK}}