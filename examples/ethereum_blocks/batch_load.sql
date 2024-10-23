insert into {{.CHAIN}}_blocks
select * from {{.CHAIN}}_blocks_transformed_{{.START}}_{{.END}}