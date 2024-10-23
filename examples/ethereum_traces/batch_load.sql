insert into {{.CHAIN}}_traces 
select * from {{.CHAIN}}_traces_transformed_{{.START}}_{{.END}}