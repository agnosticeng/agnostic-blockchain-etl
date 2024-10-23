insert into {{.CHAIN}}_logs 
select * from {{.CHAIN}}_logs_transformed_{{.START}}_{{.END}}