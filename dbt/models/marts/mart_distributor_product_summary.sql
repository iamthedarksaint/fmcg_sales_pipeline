
with transactions as (
    select
        t.transaction_id,
        t.product_id,
        t.distributor_id,
        t.transaction_date,
        t.revenue_ngn,
        t.gross_profit_ngn,
        t.quantity,
        t.is_returned,
        t.has_missing_distributor,
        d.distributor_name,
        d.region,
        d.outlet_type,
        p.product_name,
        p.category
    from {{ ref('stg_transactions') }} t
    left join {{ ref('stg_distributors') }} d using (distributor_id)
    left join {{ ref('stg_products') }} p using (product_id)
),

distributor_summary as (
    select
        distributor_id,
        distributor_name,
        region,
        outlet_type,
        count(*)                                            as total_transactions,
        sum(case when is_returned then 1 else 0 end)        as returned_transactions,
        round(
            sum(case when is_returned then 1 else 0 end)::numeric
            / nullif(count(*), 0) * 100, 2
        )                                                   as return_rate_pct,
        sum(case when not is_returned then revenue_ngn end) as total_revenue_ngn,
        sum(case when not is_returned then gross_profit_ngn end) as total_gross_profit_ngn,
        sum(case when not is_returned then quantity end)    as total_units_sold
    from transactions
    group by 1, 2, 3, 4
),

product_summary as (
    select
        product_id,
        product_name,
        category,
        count(*)                                            as total_transactions,
        sum(case when not is_returned then revenue_ngn end) as total_revenue_ngn,
        sum(case when not is_returned then gross_profit_ngn end) as total_gross_profit_ngn,
        sum(case when not is_returned then quantity end)    as total_units_sold,
        round(
            sum(case when not is_returned then gross_profit_ngn end)::numeric
            / nullif(sum(case when not is_returned then revenue_ngn end), 0) * 100, 2
        )                                                   as gross_margin_pct
    from transactions
    group by 1, 2, 3
)

select
    d.*,
    current_timestamp as _modelled_at
from distributor_summary d
order by total_revenue_ngn desc nulls last