with localized as (
    select
        interval_start_utc,
        date_key,
        time_key,
        utc_offset_minutes,
        interval_start_utc at time zone 'Europe/Brussels' as local_clock,
        interval_start_utc at time zone 'UTC' as utc_clock
    from {{ ref('fact_load_15m') }}
)
select interval_start_utc
from localized
where date_key is distinct from to_char(local_clock::date, 'YYYYMMDD')::integer
   or time_key is distinct from to_char(local_clock::time, 'HH24MI')::integer
   or utc_offset_minutes is distinct from
       (extract(epoch from (local_clock - utc_clock)) / 60)::integer
   or extract(minute from utc_clock)::integer % 15 <> 0
   or extract(second from utc_clock) <> 0
