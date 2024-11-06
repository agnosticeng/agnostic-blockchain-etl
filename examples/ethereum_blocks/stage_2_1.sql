insert into {{.CHAIN}}_blocks
select * from {{.CHAIN}}_blocks_{{.START}}_{{.END}}