with source as (

    select * from {{ source('generated', 'gen_calendar') }}

),

renamed as (

    select

        ---------- booleans
        is_trading_day,

        ---------- dates
        cast(calendar_date as date) as calendar_date

    from source

)

select * from renamed
