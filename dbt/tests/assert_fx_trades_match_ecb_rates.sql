-- Every FX ticket re-derives from the ECB reference file the warehouse
-- ingests: the mid rate must match the stored rate to one unit in its
-- eighth decimal (the producer rounds a float64 quotient, this check a
-- numeric one, and at boundaries the adjacent digit wins), and the
-- client's cost must equal mid plus the house margin within the cent the
-- stored buy amount's own rounding introduces. The euro is the file's
-- implicit base, so only euro legs may resolve no rate; any other
-- currency without one is a failure, not a default.

with fx_trades as (

    select * from {{ ref('fct_fx_trades') }}

),

rates as (

    select
        currency,
        rate_date,
        units_per_eur
    from {{ ref('stg_ecb_fxref__rates') }}

),

currency_days as (

    select
        sell_currency as currency,
        trade_date
    from fx_trades

    union distinct

    select
        buy_currency as currency,
        trade_date
    from fx_trades

),

resolved_rates as (

    select
        currency_days.currency,
        currency_days.trade_date,
        rates.units_per_eur
    from currency_days
    left join rates
        on currency_days.currency = rates.currency
            and currency_days.trade_date >= rates.rate_date
    qualify row_number() over (
            partition by currency_days.currency, currency_days.trade_date
            order by rates.rate_date desc
        ) = 1

),

checked as (

    select
        fx_trades.fx_trade_id,
        fx_trades.trade_date,
        fx_trades.sell_currency,
        fx_trades.buy_currency,
        fx_trades.reference_rate,
        fx_trades.sell_amount,
        fx_trades.buy_amount,
        buy_rates.units_per_eur as buy_rate,
        sell_rates.units_per_eur as sell_rate,
        round(
            coalesce(buy_rates.units_per_eur, 1)
            / coalesce(sell_rates.units_per_eur, 1),
            8
        ) as expected_reference_rate,
        round(
            fx_trades.buy_amount
            / (
                coalesce(buy_rates.units_per_eur, 1)
                / coalesce(sell_rates.units_per_eur, 1)
            )
            * (1 + fx_trades.margin_bp / 10000),
            2
        ) as expected_sell_amount
    from fx_trades
    left join resolved_rates as buy_rates
        on fx_trades.buy_currency = buy_rates.currency
            and fx_trades.trade_date = buy_rates.trade_date
    left join resolved_rates as sell_rates
        on fx_trades.sell_currency = sell_rates.currency
            and fx_trades.trade_date = sell_rates.trade_date

)

select *
from checked
where (buy_currency != 'EUR' and buy_rate is null)
    or (sell_currency != 'EUR' and sell_rate is null)
    or abs(reference_rate - expected_reference_rate) > 0.000000015
    or abs(sell_amount - expected_sell_amount) > 0.02
