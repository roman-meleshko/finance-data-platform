-- The derived book against the independently published statement, compared
-- both ways at every month-end cell: a derived position the statement lacks,
-- a published position the derivation lacks, or two quantities that
-- disagree all fail. Absence counts as zero, so a sold-out position matches
-- a missing statement row.

with derived as (

    select
        account_id,
        isin,
        business_date,
        closing_quantity
    from {{ ref('fct_position_daily') }}
    where is_last_trading_day_of_month

),

published as (

    select
        account_id,
        isin,
        snapshot_date,
        quantity
    from {{ ref('fct_position_snapshots') }}

),

compared as (

    select
        coalesce(derived.account_id, published.account_id) as account_id,
        coalesce(derived.isin, published.isin) as isin,
        coalesce(derived.business_date, published.snapshot_date) as as_of_date,
        coalesce(derived.closing_quantity, 0) as derived_quantity,
        coalesce(published.quantity, 0) as published_quantity
    from derived
    full outer join published
        on derived.account_id = published.account_id
            and derived.isin = published.isin
            and derived.business_date = published.snapshot_date

)

select *
from compared
where derived_quantity != published_quantity
