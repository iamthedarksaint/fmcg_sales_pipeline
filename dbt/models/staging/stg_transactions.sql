
with source as (
    select * from {{ source('raw', 'fct_transactions') }}
),

cleaned as (
    select
        transaction_id,
        transaction_date::date                          as transaction_date,
        product_id,
        distributor_id,
        salesperson_id,
        quantity::integer                               as quantity,
        unit_price_ngn::numeric(18,2)                  as unit_price_ngn,
        discount_pct::numeric(5,2)                     as discount_pct,
        discount_amount_ngn::numeric(18,2)              as discount_amount_ngn,
        revenue_ngn::numeric(18,2)                      as revenue_ngn,
        cogs_ngn::numeric(18,2)                         as cogs_ngn,
        gross_profit_ngn::numeric(18,2)                 as gross_profit_ngn,
        payment_method,
        delivery_status,
        transaction_status,
        notes,
        has_missing_distributor::boolean                as has_missing_distributor,
        _is_returned::boolean                           as is_returned,
        _loaded_at                              as loaded_at    
    from source
)

select * from cleaned