with source as (

    select * from {{ source('generated', 'gen_cash_event') }}

),

renamed as (

    select

        ---------- ids
        event_id as cash_event_id,
        account_id,
        isin,

        ---------- strings
        event_type,
        currency as event_currency,

        ---------- numerics
        cast(amount as numeric) as amount,

        ---------- dates
        cast(event_date as date) as event_date,
        cast(entitlement_date as date) as entitlement_date

    from source

)

select * from renamed
