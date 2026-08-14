with cash_events as (

    select * from {{ ref('stg_generated__cash_events') }}

),

dim_instruments as (

    select
        isin,
        instrument_version_sk,
        valid_from,
        valid_to
    from {{ ref('dim_instruments') }}

),

wide_cash_events as (

    select

        ---------- ids
        cash_events.cash_event_id,
        cash_events.account_id,
        cash_events.isin,
        instruments.instrument_version_sk,

        ---------- strings
        cash_events.event_type,
        cash_events.event_currency,

        ---------- numerics
        cash_events.amount,

        ---------- dates
        cash_events.event_date,
        cash_events.entitlement_date

    from cash_events
    left join dim_instruments as instruments
        on cash_events.isin = instruments.isin
            and cash_events.event_date >= instruments.valid_from
            and cash_events.event_date <= instruments.valid_to

)

select * from wide_cash_events
