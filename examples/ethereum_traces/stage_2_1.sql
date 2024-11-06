insert into {{.CHAIN}}_traces
select * from {{.CHAIN}}_traces_{{.START}}_{{.END}}