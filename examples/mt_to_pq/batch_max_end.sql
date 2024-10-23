select 
    toUInt64(coalesce(toDate(max(timestamp)), 0)) as max_end 
from {{.SOURCE_TABLE}}