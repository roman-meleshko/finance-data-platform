with fx_trades as (

    select * from {{ ref('stg_generated__fx_trades') }}

),

wide_fx_trades as (

    select

        ---------- ids
        fx_trade_id,
        account_id,
        related_trade_id,

        ---------- strings
        sell_currency,
        buy_currency,

        ---------- numerics
        sell_amount,
        buy_amount,
        reference_rate,
        margin_bp,

        ---------- dates
        trade_date

    from fx_trades

)

select * from wide_fx_trades
