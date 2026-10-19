with fact as (
    select * from {{ ref('fact_load_15m') }}
),
reference as (
    select min(quality_as_of_utc) as first_as_of,
           max(quality_as_of_utc) as last_as_of
    from fact
)
select f.interval_start_utc
from fact as f
cross join reference as r
where r.first_as_of is distinct from r.last_as_of
   or (f.total_load_mw is null and f.total_load_quality_status is distinct from
       case when f.interval_start_utc + interval '15 minutes' > f.quality_as_of_utc
           then 'future' else 'missing' end)
   or (f.total_load_mw is not null and f.total_load_quality_status is distinct from
       case when f.total_load_mw < 0 then 'suspect' else 'available' end)
   or (f.week_ahead_forecast_mw is null and f.week_ahead_quality_status is distinct from
       case when f.interval_start_utc + interval '15 minutes' > f.quality_as_of_utc
           then 'future' else 'missing' end)
   or (f.week_ahead_forecast_mw is not null and f.week_ahead_quality_status is distinct from
       case when f.week_ahead_forecast_mw < 0 then 'suspect' else 'available' end)
   or (f.most_recent_forecast_mw is null and f.most_recent_quality_status is distinct from
       case when f.interval_start_utc + interval '15 minutes' > f.quality_as_of_utc
           then 'future' else 'missing' end)
   or (f.most_recent_forecast_mw is not null and f.most_recent_quality_status is distinct from
       case when f.most_recent_forecast_mw < 0
           or f.most_recent_interval_status <> 'valid'
           or f.most_recent_anomaly_code is not null then 'suspect' else 'available' end)
   or (f.day_ahead_forecast_mw is null and f.day_ahead_quality_status is distinct from
       case when f.interval_start_utc + interval '15 minutes' > f.quality_as_of_utc
           then 'future' else 'missing' end)
   or (f.day_ahead_forecast_mw is not null and f.day_ahead_quality_status is distinct from
       case when f.day_ahead_forecast_mw < 0
           or f.day_ahead_interval_status <> 'valid' then 'suspect' else 'available' end)
