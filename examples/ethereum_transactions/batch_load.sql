insert into {{.CHAIN}}_transactions 
select * from {{.CHAIN}}_transactions_transformed_{{.START}}_{{.END}}