with actual as (
    select
        count(*) as slot_count,
        min(time_local) as first_time,
        max(time_local) as last_time
    from {{ ref('dim_time') }}
),
missing as (
    select slot_number
    from generate_series(0, 95) as slots(slot_number)
    left join {{ ref('dim_time') }} as d
        on d.time_local = make_time(slot_number / 4, (slot_number % 4) * 15, 0)
    where d.time_local is null
)
select 'count_or_bounds' as failure
from actual
where slot_count <> 96
   or first_time is distinct from time '00:00'
   or last_time is distinct from time '23:45'

union all

select 'missing_slot' as failure
from missing
