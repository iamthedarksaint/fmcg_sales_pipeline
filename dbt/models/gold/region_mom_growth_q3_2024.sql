-- Q2: Region with highest Month-over-Month revenue growth in Q3 2024
-- Q3 = July, August, September 2024
-- Includes June to calculate MoM for July
-- MoM growth = (current month - prior month) / prior month * 100

with monthly_revenue as (
    select
        d.region,
        date_trunc('month', t.transaction_date)::date  as month_start,
        sum(t.revenue_ngn)                              as revenue_ngn
    from {{ ref('stg_transactions') }} t
    join {{ ref('stg_distributors') }} d using (distributor_id)
    where
        not t.is_returned
        and t.transaction_date >= '2024-06-01'
        and t.transaction_date <  '2024-10-01'
    group by 1, 2
),

mom as (
    select
        region,
        month_start,
        revenue_ngn,
        lag(revenue_ngn) over (
            partition by region order by month_start
        )                                               as prev_revenue_ngn,
        round(
            (revenue_ngn - lag(revenue_ngn) over (
                partition by region order by month_start
            ))
            / nullif(lag(revenue_ngn) over (
                partition by region order by month_start
            ), 0) * 100,
        2)                                              as mom_growth_pct
    from monthly_revenue
)

select
    region,
    month_start,
    revenue_ngn,
    prev_revenue_ngn,
    mom_growth_pct
from mom
where
    month_start between '2024-07-01' and '2024-09-01'
    and mom_growth_pct is not null
order by mom_growth_pct desc
limit 1