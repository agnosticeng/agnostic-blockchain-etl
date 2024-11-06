insert into {{.CHAIN}}_transactions 
select * from {{.CHAIN}}_transactions_{{.START}}_{{.END}}