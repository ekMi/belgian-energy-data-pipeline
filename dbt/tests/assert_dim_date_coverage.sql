with source_bounds as (
    select
        min((timestamp_utc at time zone 'Europe/Brussels')::date) as first_date,
        max((timestamp_utc at time zone 'Europe/Brussels')::date) as last_date
    from {{ source('elia', 'elia_load') }}
),
actual as (
    select
        min(date_local) as first_date,
        max(date_local) as last_date,
        count(*) as days
    from {{ ref('dim_date') }}
),
gaps as (
    select date_local
    from (
        select
            date_local,
            lag(date_local) over (order by date_local) as previous_date
        from {{ ref('dim_date') }}
    ) as ordered_dates
    where previous_date is not null and date_local <> previous_date + 1
)
select 'bounds_or_count' as failure
from source_bounds cross join actual
where source_bounds.first_date is distinct from actual.first_date
   or source_bounds.last_date is distinct from actual.last_date
   or actual.days <> coalesce(source_bounds.last_date - source_bounds.first_date + 1, 0)

union all

select 'gap' as failure
from gaps
