with movements as (

    select * from {{ ref('int_generated__position_movements') }}

),

calendar_days as (

    select
        date_key,
        is_last_trading_day_of_month
    from {{ ref('dim_dates') }}
    where is_trading_day

),

prices as (

    select
        isin,
        business_date,
        price,
        price_currency,
        price_source,
        price_convention
    from {{ ref('stg_generated__prices') }}

),

dim_instruments as (

    select
        isin,
        instrument_version_sk,
        valid_from,
        valid_to,
        nominal_value_per_unit,
        price_multiplier
    from {{ ref('dim_instruments') }}

),

holdings as (

    select
        account_id,
        isin,
        trading_venue_mic,
        min(movement_date) as first_movement_date
    from movements
    group by account_id, isin, trading_venue_mic

),

spine as (

    select
        holdings.account_id,
        holdings.isin,
        holdings.trading_venue_mic,
        calendar_days.date_key as business_date,
        calendar_days.is_last_trading_day_of_month
    from holdings
    inner join calendar_days
        on holdings.first_movement_date <= calendar_days.date_key

),

daily_deltas as (

    select
        account_id,
        isin,
        movement_date,
        sum(quantity_delta) as quantity_delta
    from movements
    group by account_id, isin, movement_date

),

accumulated as (

    select
        spine.account_id,
        spine.isin,
        spine.trading_venue_mic,
        spine.business_date,
        spine.is_last_trading_day_of_month,
        sum(coalesce(daily_deltas.quantity_delta, 0)) over (
            partition by spine.account_id, spine.isin
            order by spine.business_date
            rows between unbounded preceding and current row
        ) as closing_quantity
    from spine
    left join daily_deltas
        on spine.account_id = daily_deltas.account_id
            and spine.isin = daily_deltas.isin
            and spine.business_date = daily_deltas.movement_date

),

held_days as (

    select distinct
        isin,
        business_date
    from accumulated

),

-- the price feed carries single-day holes (411 instruments miss exactly
-- one print each), so valuation carries the last known price forward,
-- the same thing a custodian statement does with a stale price
prices_filled as (

    select
        held_days.isin,
        held_days.business_date,
        last_value(prices.price ignore nulls) over (price_window) as price,
        last_value(prices.price_currency ignore nulls)
            over (price_window) as price_currency,
        coalesce(prices.price_source, 'carried_forward') as price_source,
        last_value(prices.price_convention ignore nulls)
            over (price_window) as price_convention
    from held_days
    left join prices
        on held_days.isin = prices.isin
            and held_days.business_date = prices.business_date
    window price_window as (
        partition by held_days.isin
        order by held_days.business_date
        rows between unbounded preceding and current row
    )

),

valued as (

    select

        ---------- ids
        accumulated.account_id,
        accumulated.isin,
        accumulated.trading_venue_mic,
        instruments.instrument_version_sk,

        ---------- numerics
        accumulated.closing_quantity,
        prices_filled.price,
        case prices_filled.price_convention
            when 'percent_of_par'
                then round(
                        accumulated.closing_quantity
                        * instruments.nominal_value_per_unit
                        * prices_filled.price / 100,
                        2
                    )
            else round(
                    accumulated.closing_quantity
                    * prices_filled.price
                    * coalesce(instruments.price_multiplier, 1),
                    2
                )
        end as market_value_local,

        ---------- strings
        prices_filled.price_currency,
        prices_filled.price_source,

        ---------- booleans
        accumulated.is_last_trading_day_of_month,

        ---------- dates
        accumulated.business_date

    from accumulated
    left join prices_filled
        on accumulated.isin = prices_filled.isin
            and accumulated.business_date = prices_filled.business_date
    left join dim_instruments as instruments
        on accumulated.isin = instruments.isin
            and accumulated.business_date >= instruments.valid_from
            and accumulated.business_date <= instruments.valid_to

)

select * from valued
