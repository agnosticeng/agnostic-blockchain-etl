insert into {{.CHAIN}}_logs
select * from {{.CHAIN}}_logs_{{.START}}_{{.END}}