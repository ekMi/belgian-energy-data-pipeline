select time_local
from {{ ref('dim_time') }}
where time_key is distinct from hour * 100 + minute
   or time_local is distinct from make_time(hour, minute, 0)
   or hour is distinct from extract(hour from time_local)::integer
   or minute is distinct from extract(minute from time_local)::integer
   or quarter_hour is distinct from (minute / 15 + 1)
   or time_label is distinct from to_char(time_local, 'HH24')
   or hour not between 0 and 23
   or quarter_hour not between 1 and 4
