with source as (

    select * from {{ source('generated', 'gen_position_snapshot') }}

),

renamed as (

    select

        ---------- ids
        account_id,
        isin,

        ---------- strings
        currency as position_currency,

        ---------- numerics
        quantity,

        ---------- booleans
        is_month_end,

        ---------- dates
        cast(snapshot_date as date) as snapshot_date

    from source

)

select * from renamed
