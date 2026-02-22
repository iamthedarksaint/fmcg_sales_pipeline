-- Q4: Distributor with highest return rate
-- Return rate = returned transactions / total transactions
-- Rows with missing distributor_id are excluded as they cannot be attributed

select
    t.distributor_id,
    d.distributor_name,
    d.region,
    d.outlet_type,
    count(*)                                                as total_transactions,
    sum(case when t.is_returned then 1 else 0 end)          as returned_transactions,
    round(
        sum(case when t.is_returned then 1 else 0 end)::numeric
        / nullif(count(*), 0) * 100,
    2)                                                      as return_rate_pct
from {{ ref('stg_transactions') }} t
left join {{ ref('stg_distributors') }} d using (distributor_id)
where not t.has_missing_distributor
group by 1, 2, 3, 4
order by return_rate_pct desc
limit 1