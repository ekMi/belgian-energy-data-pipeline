-- Fail instead of silently classifying out-of-range dates as business days.
select date_local
from {{ ref('dim_date') }}
where date_local < date '2015-01-01' or date_local > date '2040-12-31'

union all

-- Exactly ten distinct holidays for every supported year, no extra dates.
select make_date(years.year, 1, 1)
from generate_series(2015, 2040) as years(year)
left join {{ ref('belgian_public_holidays') }} as h
    on extract(year from h.holiday_date) = years.year
group by years.year
having count(*) filter (where h.holiday_date is not null) <> 10
    or count(distinct h.holiday_date) <> 10

union all

select holiday_date
from {{ ref('belgian_public_holidays') }}
where holiday_date < date '2015-01-01' or holiday_date > date '2040-12-31'
