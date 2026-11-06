{{ config(materialized='table') }}

with source_bounds as (
    select
        min((timestamp_utc at time zone 'Europe/Brussels')::date) as first_date,
        max((timestamp_utc at time zone 'Europe/Brussels')::date) as last_date
    from {{ source('elia', 'elia_load') }}
),

calendar as (
    select day::date as date_local
    from source_bounds
    cross join lateral generate_series(
        first_date::timestamp,
        last_date::timestamp,
        interval '1 day'
    ) as days(day)
),

enriched_calendar as (
    select
        c.date_local,
        extract(isodow from c.date_local) in (6, 7) as is_weekend,
        h.holiday_name,
        h.holiday_date is not null as is_public_holiday
    from calendar as c
    left join {{ ref('belgian_public_holidays') }} as h
        on c.date_local = h.holiday_date
)

select
    to_char(date_local, 'YYYYMMDD')::integer as date_key,
    date_local,
    extract(year from date_local)::integer as year,
    extract(quarter from date_local)::integer as quarter,
    extract(month from date_local)::integer as month,
    to_char(date_local, 'FMMonth') as month_name,
    extract(isoyear from date_local)::integer as iso_year,
    extract(week from date_local)::integer as iso_week,
    extract(day from date_local)::integer as day_of_month,
    extract(isodow from date_local)::integer as day_of_week,
    is_weekend,
    is_public_holiday,
    holiday_name,

    not is_weekend and not is_public_holiday as is_business_day,

    case
        when is_public_holiday then 'Férié'
        when is_weekend then 'Week-end'
        else 'Jour ouvré'
    end as day_type,

    case
        when extract(month from date_local) in (12, 1, 2) then 'winter'
        when extract(month from date_local) in (3, 4, 5) then 'spring'
        when extract(month from date_local) in (6, 7, 8) then 'summer'
        else 'autumn'
    end as season
from enriched_calendar
