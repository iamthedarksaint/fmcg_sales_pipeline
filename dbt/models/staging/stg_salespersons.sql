with source as (
    select * from {{ source('raw', 'dim_salespersons') }}
)
select
    salesperson_id,
    salesperson_name,
    region,
    team,
    hire_date::date             as hire_date,
    monthly_target_ngn::numeric(18,2) as monthly_target_ngn,
    loaded_at
from source