with targets as (
    select
        salesperson_id,
        year,
        month,
        month_start_date,
        region,
        target_revenue_ngn,
        actual_revenue_ngn,
        achievement_pct
    from {{ ref('stg_monthly_targets') }}
),

salespersons as (
    select
        salesperson_id,
        salesperson_name,
        team,
        hire_date
    from {{ ref('stg_salespersons') }}
),

-- Transaction-level revenue per salesperson per month (excludes returns)
txn_revenue as (
    select
        salesperson_id,
        date_trunc('month', transaction_date)::date as txn_month,
        sum(revenue_ngn)                             as txn_revenue_ngn,
        count(*)                                     as total_transactions,
        sum(gross_profit_ngn)                        as gross_profit_ngn
    from {{ ref('stg_transactions') }}
    where not is_returned
    group by 1, 2
),

final as (
    select
        t.salesperson_id,
        s.salesperson_name,
        s.team,
        t.region,
        t.year,
        t.month,
        t.month_start_date,
        t.target_revenue_ngn,
        t.actual_revenue_ngn,
        t.achievement_pct,
        coalesce(r.txn_revenue_ngn, 0)      as verified_txn_revenue_ngn,
        coalesce(r.total_transactions, 0)   as total_transactions,
        coalesce(r.gross_profit_ngn, 0)     as gross_profit_ngn,
        -- Month-over-Month revenue change
        t.actual_revenue_ngn - lag(t.actual_revenue_ngn) over (
            partition by t.salesperson_id order by t.month_start_date
        )                                   as mom_revenue_change_ngn
    from targets t
    left join salespersons s using (salesperson_id)
    left join txn_revenue r
        on t.salesperson_id = r.salesperson_id
        and t.month_start_date = r.txn_month
)

select * from final