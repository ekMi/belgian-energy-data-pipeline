select date_local
from {{ ref('dim_date') }}
where date_key is distinct from to_char(date_local, 'YYYYMMDD')::integer
   or year is distinct from extract(year from date_local)::integer
   or quarter is distinct from extract(quarter from date_local)::integer
   or month is distinct from extract(month from date_local)::integer
   or month_name is distinct from to_char(date_local, 'FMMonth')
   or iso_year is distinct from extract(isoyear from date_local)::integer
   or iso_week is distinct from extract(week from date_local)::integer
   or day_of_month is distinct from extract(day from date_local)::integer
   or day_of_week is distinct from extract(isodow from date_local)::integer
   or is_weekend is distinct from (extract(isodow from date_local) in (6, 7))
   or season is distinct from case
       when extract(month from date_local) in (12, 1, 2) then 'winter'
       when extract(month from date_local) in (3, 4, 5) then 'spring'
       when extract(month from date_local) in (6, 7, 8) then 'summer'
       else 'autumn'
   end
   or iso_week not between 1 and 53
