-- Q1: Top 5 products by total revenue in 2024
-- Excludes returned transactions

select
    p.product_id,
    p.product_name,
    p.category,
    sum(t.revenue_ngn)          as total_revenue_ngn,
    sum(t.quantity)             as total_units_sold,
    count(t.transaction_id)     as total_transactions
from {{ ref('stg_transactions') }} t
join {{ ref('stg_products') }} p using (product_id)
where
    not t.is_returned
    and extract(year from t.transaction_date) = 2024
group by 1, 2, 3
order by total_revenue_ngn desc
limit 5