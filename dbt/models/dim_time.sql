{{ config(materialized='table') }}

with slots as (
    select slot_number
    from generate_series(0, 95) as slot_numbers(slot_number)
),

clock as (
    select
        (slot_number / 4)::integer as hour,
        ((slot_number % 4) * 15)::integer as minute
    from slots
)

select
    hour * 100 + minute as time_key,
    make_time(hour, minute, 0) as time_local,
    hour,
    minute,
    minute / 15 + 1 as quarter_hour,
    lpad(hour::text, 2, '0') as time_label
from clock
