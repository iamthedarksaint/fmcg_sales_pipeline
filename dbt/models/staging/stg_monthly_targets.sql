with source as (
    select * from {{ source('raw', 'fct_monthly_targets') }}
)
select
    record_id,
    salesperson_id,
    year::integer                           as year,
    month::integer                          as month,
    make_date(year::int, month::int, 1)     as month_start_date,
    region,
    target_revenue_ngn::numeric(18,2)       as target_revenue_ngn,
    actual_revenue_ngn::numeric(18,2)       as actual_revenue_ngn,
    achievement_pct::numeric(8,2)           as achievement_pct,
    _loaded_at
from source
