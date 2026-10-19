{{ config(materialized='table') }}

with localized as (
    select
        timestamp_utc as interval_start_utc,
        timestamp_utc at time zone 'Europe/Brussels' as interval_start_local,
        timestamp_utc at time zone 'UTC' as interval_start_utc_clock,
        cast('{{ var("quality_as_of_utc", run_started_at.isoformat()) }}' as timestamptz)
            as quality_as_of_utc,
        total_load_mw,
        most_recent_forecast_mw,
        most_recent_p10_mw,
        most_recent_p90_mw,
        day_ahead_forecast_mw,
        day_ahead_p10_mw,
        day_ahead_p90_mw,
        week_ahead_forecast_mw
    from {{ source('elia', 'elia_load') }}
),

assessed as (
    select
        *,
        case
            when most_recent_forecast_mw is null
                or most_recent_p10_mw is null
                or most_recent_p90_mw is null then 'incomplete'
            when most_recent_p10_mw < 0 or most_recent_p90_mw < 0
                then 'negative_bound'
            when most_recent_p10_mw > most_recent_p90_mw
                then 'reversed_bounds'
            when most_recent_forecast_mw < most_recent_p10_mw
                or most_recent_forecast_mw > most_recent_p90_mw
                then 'forecast_outside_bounds'
            else 'valid'
        end as most_recent_interval_status,
        case
            when day_ahead_forecast_mw is null
                or day_ahead_p10_mw is null
                or day_ahead_p90_mw is null then 'incomplete'
            when day_ahead_p10_mw < 0 or day_ahead_p90_mw < 0
                then 'negative_bound'
            when day_ahead_p10_mw > day_ahead_p90_mw
                then 'reversed_bounds'
            when day_ahead_forecast_mw < day_ahead_p10_mw
                or day_ahead_forecast_mw > day_ahead_p90_mw
                then 'forecast_outside_bounds'
            else 'valid'
        end as day_ahead_interval_status,
        case
            when interval_start_utc = timestamptz '2015-06-24 03:30:00+00'
                and most_recent_forecast_mw = 846.0
                then 'known_2015_06_24_most_recent_846_mw'
        end as most_recent_anomaly_code
    from localized
)

select
    a.interval_start_utc,
    d.date_key,
    t.time_key,
    (extract(epoch from (
        a.interval_start_local - a.interval_start_utc_clock
    )) / 60)::integer as utc_offset_minutes,
    a.quality_as_of_utc,
    a.total_load_mw,
    a.most_recent_forecast_mw,
    a.most_recent_p10_mw,
    a.most_recent_p90_mw,
    a.day_ahead_forecast_mw,
    a.day_ahead_p10_mw,
    a.day_ahead_p90_mw,
    a.week_ahead_forecast_mw,
    a.most_recent_interval_status,
    a.day_ahead_interval_status,
    a.most_recent_anomaly_code,
    case
        when a.total_load_mw is null
            and a.interval_start_utc + interval '15 minutes' > a.quality_as_of_utc
            then 'future'
        when a.total_load_mw is null then 'missing'
        when a.total_load_mw < 0 then 'suspect'
        else 'available'
    end as total_load_quality_status,
    case
        when a.most_recent_forecast_mw is null
            and a.interval_start_utc + interval '15 minutes' > a.quality_as_of_utc
            then 'future'
        when a.most_recent_forecast_mw is null then 'missing'
        when a.most_recent_forecast_mw < 0
            or a.most_recent_interval_status <> 'valid'
            or a.most_recent_anomaly_code is not null then 'suspect'
        else 'available'
    end as most_recent_quality_status,
    case
        when a.day_ahead_forecast_mw is null
            and a.interval_start_utc + interval '15 minutes' > a.quality_as_of_utc
            then 'future'
        when a.day_ahead_forecast_mw is null then 'missing'
        when a.day_ahead_forecast_mw < 0
            or a.day_ahead_interval_status <> 'valid' then 'suspect'
        else 'available'
    end as day_ahead_quality_status,
    case
        when a.week_ahead_forecast_mw is null
            and a.interval_start_utc + interval '15 minutes' > a.quality_as_of_utc
            then 'future'
        when a.week_ahead_forecast_mw is null then 'missing'
        when a.week_ahead_forecast_mw < 0 then 'suspect'
        else 'available'
    end as week_ahead_quality_status
from assessed as a
left join {{ ref('dim_date') }} as d
    on d.date_local = a.interval_start_local::date
left join {{ ref('dim_time') }} as t
    on t.time_local = a.interval_start_local::time
