-- Null-safe comparisons also reject missing boolean/category values.
select d.date_local
from {{ ref('dim_date') }} as d
left join {{ ref('belgian_public_holidays') }} as h
    on d.date_local = h.holiday_date
where d.is_public_holiday is distinct from (h.holiday_date is not null)
   or d.holiday_name is distinct from h.holiday_name
   or d.is_business_day is distinct from (
       extract(isodow from d.date_local) not in (6, 7)
       and h.holiday_date is null
   )
   or d.day_type is distinct from case
       when h.holiday_date is not null then 'Férié'
       when extract(isodow from d.date_local) in (6, 7) then 'Week-end'
       else 'Jour ouvré'
   end
