select 
    max(block_number) + 1 as start_block
from ethereum_logs_extracted_{{.START_BLOCK}}_{{.END_BLOCK}} 