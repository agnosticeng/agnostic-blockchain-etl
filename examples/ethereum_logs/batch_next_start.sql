select 
    max(block_number) + 1 as start
from {{.CHAIN}}_logs_extracted_{{.START}}_{{.END}} 