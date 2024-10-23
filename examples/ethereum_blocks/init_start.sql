select 
    max(number) + 1 as start_block
from {{.CHAIN}}_blocks
