insert into sink 
select * from buffer_{{.START}}_{{.END}}