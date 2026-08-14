with source as (

    select * from {{ source('generated', 'gen_fx_trade') }}

),

renamed as (

    select

        ---------- ids
        fx_id as fx_trade_id,
        account_id,
        related_trade_id,

        ---------- strings
        sell_currency,
        buy_currency,

        ---------- numerics
        cast(sell_amount as numeric) as sell_amount,
        cast(buy_amount as numeric) as buy_amount,
        cast(reference_rate as numeric) as reference_rate,
        cast(margin_bp as numeric) as margin_bp,

        ---------- dates
        cast(trade_date as date) as trade_date

    from source

)

select * from renamed
