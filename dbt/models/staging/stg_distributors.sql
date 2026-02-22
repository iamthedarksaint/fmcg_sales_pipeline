with source as (
    select * from {{ source('raw', 'dim_distributors') }}
)
select
    distributor_id,
    distributor_name,
    region,
    city,
    outlet_type,
    onboarding_date::date   as onboarding_date,
    is_active::boolean      as is_active,
    loaded_at
from source