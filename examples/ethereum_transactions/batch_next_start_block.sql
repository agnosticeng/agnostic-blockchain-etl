select 
    max(block_number) + 1 as start_block
from ethereum_transactions_extracted_{{.START_BLOCK}}_{{.END_BLOCK}} 