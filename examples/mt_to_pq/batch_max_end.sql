select 
    toUInt64(coalesce(toDate(max(timestamp))-1, 0)) as max_end 
from {{.SOURCE_TABLE}}