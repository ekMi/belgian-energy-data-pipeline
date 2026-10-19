select coalesce(s.timestamp_utc, f.interval_start_utc) as failed_interval_utc
from {{ source('elia', 'elia_load') }} as s
full outer join {{ ref('fact_load_15m') }} as f
    on f.interval_start_utc = s.timestamp_utc
where s.timestamp_utc is null
   or f.interval_start_utc is null
   or s.total_load_mw is distinct from f.total_load_mw
   or s.most_recent_forecast_mw is distinct from f.most_recent_forecast_mw
   or s.most_recent_p10_mw is distinct from f.most_recent_p10_mw
   or s.most_recent_p90_mw is distinct from f.most_recent_p90_mw
   or s.day_ahead_forecast_mw is distinct from f.day_ahead_forecast_mw
   or s.day_ahead_p10_mw is distinct from f.day_ahead_p10_mw
   or s.day_ahead_p90_mw is distinct from f.day_ahead_p90_mw
   or s.week_ahead_forecast_mw is distinct from f.week_ahead_forecast_mw
