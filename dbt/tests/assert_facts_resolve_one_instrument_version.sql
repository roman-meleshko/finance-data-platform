-- Every fact key and date must resolve exactly one dimension version: zero
-- rows is a timeline gap, two is an overlap, both caught through the same
-- range join the fact models use.

with fact_keys as (

    select
        isin,
        trading_venue_mic,
        trade_date as as_of_date
    from {{ ref('stg_generated__trades') }}

    union distinct

    select
        isin,
        trading_venue_mic,
        transfer_date as as_of_date
    from {{ ref('stg_generated__transfers') }}

),

resolved as (

    select
        f.isin,
        f.trading_venue_mic,
        f.as_of_date,
        count(d.instrument_version_sk) as version_rows
    from fact_keys as f
    left join {{ ref('dim_instruments') }} as d
        on f.isin = d.isin
            and f.trading_venue_mic = d.trading_venue_mic
            and f.as_of_date between d.valid_from and d.valid_to
    group by f.isin, f.trading_venue_mic, f.as_of_date

)

select *
from resolved
where version_rows != 1
