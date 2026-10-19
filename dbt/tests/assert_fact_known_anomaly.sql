with source_anomaly as (
    select timestamp_utc
    from {{ source('elia', 'elia_load') }}
    where timestamp_utc = timestamptz '2015-06-24 03:30:00+00'
      and most_recent_forecast_mw = 846.0
)
select source_anomaly.timestamp_utc
from source_anomaly
left join {{ ref('fact_load_15m') }} as f
    on f.interval_start_utc = source_anomaly.timestamp_utc
where f.interval_start_utc is null
   or f.most_recent_forecast_mw is distinct from 846.0
   or f.most_recent_quality_status is distinct from 'suspect'
   or f.most_recent_interval_status is distinct from 'forecast_outside_bounds'
   or f.most_recent_anomaly_code is distinct from 'known_2015_06_24_most_recent_846_mw'

union all

select f.interval_start_utc
from {{ ref('fact_load_15m') }} as f
where f.most_recent_anomaly_code is not null
  and (f.interval_start_utc <> timestamptz '2015-06-24 03:30:00+00'
       or f.most_recent_forecast_mw is distinct from 846.0)
