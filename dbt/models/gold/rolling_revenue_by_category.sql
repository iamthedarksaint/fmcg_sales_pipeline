-- Q5: Rolling 3-month revenue trend by product category
-- Uses window function: SUM over current + 2 preceding months
-- Excludes returned transactions

with monthly_category_revenue as (
    select
        p.category,
        date_trunc('month', t.transaction_date)::date   as month_start,
        sum(t.revenue_ngn)                               as monthly_revenue_ngn
    from {{ ref('stg_transactions') }} t
    join {{ ref('stg_products') }} p using (product_id)
    where not t.is_returned
    group by 1, 2
)

select
    category,
    month_start,
    monthly_revenue_ngn,
    round(
        sum(monthly_revenue_ngn) over (
            partition by category
            order by month_start
            rows between 2 preceding and current row
        ),
    2)                              as rolling_3m_revenue_ngn,
    round(
        avg(monthly_revenue_ngn) over (
            partition by category
            order by month_start
            rows between 2 preceding and current row
        ),
    2)                              as rolling_3m_avg_revenue_ngn
from monthly_category_revenue
order by category, month_start