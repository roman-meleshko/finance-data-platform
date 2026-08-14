with transfers as (

    select * from {{ ref('stg_generated__transfers') }}

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

wide_transfers as (

    select

        ---------- ids
        transfers.transfer_id,
        transfers.account_id,
        transfers.isin,
        transfers.trading_venue_mic,
        instruments.instrument_version_sk,

        ---------- strings
        transfers.transfer_direction,
        transfers.transfer_currency,

        ---------- numerics
        transfers.quantity,
        transfers.market_price,
        transfers.market_value,

        ---------- dates
        transfers.transfer_date

    from transfers
    left join dim_instruments as instruments
        on transfers.isin = instruments.isin
            and transfers.trading_venue_mic = instruments.trading_venue_mic
            and transfers.transfer_date >= instruments.valid_from
            and transfers.transfer_date <= instruments.valid_to

)

select * from wide_transfers
