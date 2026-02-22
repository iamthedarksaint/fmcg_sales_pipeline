with source as (
    select * from {{ source('raw', 'dim_date') }}
)
select
    date::date          as date,
    year::integer       as year,
    quarter::integer    as quarter,
    month::integer      as month,
    month_name,
    week::integer       as week,
    day_of_week,
    is_weekend::boolean as is_weekend,
    is_month_end::boolean as is_month_end
from source
