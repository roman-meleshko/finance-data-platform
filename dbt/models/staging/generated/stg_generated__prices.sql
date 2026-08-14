with source as (

    select * from {{ source('generated', 'gen_price') }}

),

renamed as (

    select

        ---------- ids
        isin,

        ---------- strings
        currency as price_currency,
        price_source,
        price_convention,

        ---------- numerics
        cast(price as numeric) as price,

        ---------- dates
        cast(business_date as date) as business_date

    from source

)

select * from renamed
