select 
    max(number) + 1 as start_block
from ethereum_logs_{{.START_BLOCK}}_{{.END_BLOCK}}