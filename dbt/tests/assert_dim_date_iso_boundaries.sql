-- Fixed ISO 8601 reference cases around civil-year boundaries.
with expected (date_local, iso_year, iso_week) as (
    values
        (date '2016-01-01', 2015, 53),
        (date '2018-12-31', 2019, 1),
        (date '2021-01-01', 2020, 53)
),
coverage as (
    select min(date_local) as first_date, max(date_local) as last_date
    from {{ ref('dim_date') }}
)
select expected.date_local
from expected
cross join coverage
left join {{ ref('dim_date') }} as actual using (date_local)
where expected.date_local between coverage.first_date and coverage.last_date
  and (
      actual.date_local is null
      or actual.iso_year is distinct from expected.iso_year
      or actual.iso_week is distinct from expected.iso_week
  )
