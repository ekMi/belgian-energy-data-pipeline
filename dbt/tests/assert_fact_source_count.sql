with source_count as (
    select count(*) as rows from {{ source('elia', 'elia_load') }}
),
fact_count as (
    select count(*) as rows from {{ ref('fact_load_15m') }}
)
select source_count.rows as source_rows, fact_count.rows as fact_rows
from source_count cross join fact_count
where source_count.rows <> fact_count.rows
