select 
    toUInt64(coalesce(toDate(max(timestamp)), 1) - 1) as tip
from {{.SOURCE_TABLE}}