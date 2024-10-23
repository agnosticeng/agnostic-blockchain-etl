select 
    max(block_number) + 1 as start_block
from {{.CHAIN}}_blocks_extracted_{{.START}}_{{.END}} 