with source as (

    select * from {{ source('generated', 'gen_transfer') }}

),

renamed as (

    select

        ---------- ids
        transfer_id,
        account_id,
        isin,
        mic as trading_venue_mic,

        ---------- strings
        direction as transfer_direction,
        currency as transfer_currency,

        ---------- numerics
        quantity,
        cast(market_price as numeric) as market_price,
        cast(market_value as numeric) as market_value,

        ---------- dates
        cast(transfer_date as date) as transfer_date

    from source

)

select * from renamed
