select 
    max(block_number) + 1 as start
from {{.CHAIN}}_traces_extracted_{{.START}}_{{.END}} 