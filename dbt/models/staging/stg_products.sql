with source as (
    select * from {{ source('raw', 'dim_products') }}
)
select
    product_id,
    product_name,
    category,
    unit_price_ngn::numeric(18,2)   as unit_price_ngn,
    unit_cost_ngn::numeric(18,2)    as unit_cost_ngn,
    pack_size::integer               as pack_size,
    is_active::boolean               as is_active,
    loaded_at
from source