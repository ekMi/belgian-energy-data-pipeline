-- Independent timezone reference for the two Belgian DST changes in 2025.
-- This checks the generic time dimension; it does not assert source completeness.
with utc_slots as (
    select
        'spring' as transition,
        timestamp_utc at time zone 'Europe/Brussels' as local_timestamp
    from generate_series(
        timestamptz '2025-03-29 23:00:00+00',
        timestamptz '2025-03-30 21:45:00+00',
        interval '15 minutes'
    ) as slots(timestamp_utc)

    union all

    select
        'autumn' as transition,
        timestamp_utc at time zone 'Europe/Brussels' as local_timestamp
    from generate_series(
        timestamptz '2025-10-25 22:00:00+00',
        timestamptz '2025-10-26 22:45:00+00',
        interval '15 minutes'
    ) as slots(timestamp_utc)
),
observed as (
    select
        transition,
        count(*) as utc_slot_count,
        count(distinct local_timestamp::time) as local_time_count,
        count(*) filter (where extract(hour from local_timestamp) = 2) as two_oclock_count,
        count(*) filter (where d.time_key is null) as unmatched_time_count
    from utc_slots
    left join {{ ref('dim_time') }} as d
        on d.time_local = local_timestamp::time
    group by transition
)
select transition
from observed
where (transition = 'spring' and (
    utc_slot_count <> 92 or local_time_count <> 92
    or two_oclock_count <> 0 or unmatched_time_count <> 0
)) or (transition = 'autumn' and (
    utc_slot_count <> 100 or local_time_count <> 96
    or two_oclock_count <> 8 or unmatched_time_count <> 0
))
