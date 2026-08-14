with trades as (

    select * from {{ ref('stg_generated__trades') }}

),

dim_instruments as (

    select
        isin,
        trading_venue_mic,
        instrument_version_sk,
        valid_from,
        valid_to
    from {{ ref('dim_instruments') }}

),

wide_trades as (

    select
        ---------- ids
        trades.trade_id,
        trades.account_id,
        trades.isin,
        trades.trading_venue_mic,
        instruments.instrument_version_sk,

        ---------- strings
        trades.trade_side,
        trades.trade_type,
        trades.trade_currency,

        ---------- numerics
        trades.quantity,
        trades.price,
        trades.gross_consideration,
        trades.accrued_interest,
        trades.fees,

        ---------- dates
        trades.trade_date,
        trades.settlement_date

    from trades
    left join dim_instruments as instruments
        on trades.isin = instruments.isin
            and trades.trading_venue_mic = instruments.trading_venue_mic
            and trades.trade_date >= instruments.valid_from
            and trades.trade_date <= instruments.valid_to

)

select * from wide_trades
