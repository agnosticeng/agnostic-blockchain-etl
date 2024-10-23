select 
    max(number) + 1 as start
from {{.CHAIN}}_blocks
