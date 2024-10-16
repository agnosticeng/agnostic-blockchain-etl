insert into ethereum_logs 
select * from ethereum_logs_transformed_{{.START_BLOCK}}_{{.END_BLOCK}}