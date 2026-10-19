with daily as (
    select
        date_key,
        count(*) as observations,
        count(distinct interval_start_utc) as utc_instants,
        count(distinct time_key) as local_slots,
        min(utc_offset_minutes) as min_offset,
        max(utc_offset_minutes) as max_offset
    from {{ ref('fact_load_15m') }}
    where date_key in (20250330, 20251026)
    group by date_key
),
expected as (
    select * from (values
        (20250330, 92, 92),
        (20251026, 100, 96)
    ) as dates(date_key, expected_observations, expected_local_slots)
),
repeated_autumn_slots as (
    select time_key
    from {{ ref('fact_load_15m') }}
    where date_key = 20251026 and time_key between 200 and 245
    group by time_key
    having count(*) <> 2
        or count(distinct interval_start_utc) <> 2
        or min(utc_offset_minutes) <> 60
        or max(utc_offset_minutes) <> 120
)
select 'daily_counts' as failure
from expected
left join daily using (date_key)
where daily.date_key is null
   or daily.observations <> expected.expected_observations
   or daily.utc_instants <> expected.expected_observations
   or daily.local_slots <> expected.expected_local_slots
   or daily.min_offset <> 60
   or daily.max_offset <> 120

union all

select 'repeated_autumn_slot' as failure
from repeated_autumn_slots

union all

select 'missing_repeated_autumn_slots' as failure
where (select count(*) from repeated_autumn_slots) = 0
  and (select count(distinct time_key) from {{ ref('fact_load_15m') }}
       where date_key = 20251026 and time_key between 200 and 245) <> 4
