with source as (

    select * from {{ source('generated', 'gen_trade') }}

),

renamed as (

    select

        ---------- ids
        trade_id,
        account_id,
        isin,
        mic as trading_venue_mic,

        ---------- strings
        side as trade_side,
        trade_type,
        currency as trade_currency,

        ---------- numerics
        quantity,
        cast(price as numeric) as price,
        cast(gross_consideration as numeric) as gross_consideration,
        cast(accrued_interest as numeric) as accrued_interest,
        cast(fees as numeric) as fees,

        ---------- dates
        cast(trade_date as date) as trade_date,
        cast(settlement_date as date) as settlement_date

    from source

)

select * from renamed
