-- Q3: Average target achievement % per salesperson across all months
-- achievement_pct was blank in source — computed during ETL as actual / target * 100

select
    mt.salesperson_id,
    s.salesperson_name,
    s.region,
    s.team,
    count(*)                            as months_recorded,
    round(avg(mt.achievement_pct), 2)   as avg_achievement_pct,
    round(min(mt.achievement_pct), 2)   as min_achievement_pct,
    round(max(mt.achievement_pct), 2)   as max_achievement_pct,
    round(sum(mt.actual_revenue_ngn), 2) as total_actual_revenue_ngn,
    round(sum(mt.target_revenue_ngn), 2) as total_target_revenue_ngn
from {{ ref('stg_monthly_targets') }} mt
join {{ ref('stg_salespersons') }} s using (salesperson_id)
group by 1, 2, 3, 4
order by avg_achievement_pct desc