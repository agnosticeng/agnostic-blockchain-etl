insert into {{.CHAIN}}_logs 
select * from {{.CHAIN}}_logs_transformed_{{.START_BLOCK}}_{{.END_BLOCK}}