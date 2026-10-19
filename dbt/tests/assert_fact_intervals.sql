select interval_start_utc
from {{ ref('fact_load_15m') }}
where most_recent_interval_status is distinct from case
        when most_recent_forecast_mw is null
            or most_recent_p10_mw is null or most_recent_p90_mw is null
            then 'incomplete'
        when most_recent_p10_mw < 0 or most_recent_p90_mw < 0
            then 'negative_bound'
        when most_recent_p10_mw > most_recent_p90_mw then 'reversed_bounds'
        when most_recent_forecast_mw < most_recent_p10_mw
            or most_recent_forecast_mw > most_recent_p90_mw
            then 'forecast_outside_bounds'
        else 'valid'
    end
   or day_ahead_interval_status is distinct from case
        when day_ahead_forecast_mw is null
            or day_ahead_p10_mw is null or day_ahead_p90_mw is null
            then 'incomplete'
        when day_ahead_p10_mw < 0 or day_ahead_p90_mw < 0
            then 'negative_bound'
        when day_ahead_p10_mw > day_ahead_p90_mw then 'reversed_bounds'
        when day_ahead_forecast_mw < day_ahead_p10_mw
            or day_ahead_forecast_mw > day_ahead_p90_mw
            then 'forecast_outside_bounds'
        else 'valid'
    end
