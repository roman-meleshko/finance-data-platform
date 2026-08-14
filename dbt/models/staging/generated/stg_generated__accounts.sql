with source as (

    select * from {{ source('generated', 'gen_account') }}

),

renamed as (

    select

        ---------- ids
        account_id,
        client_id,
        desk_id,
        rm_id,

        ---------- strings
        base_currency,
        mandate_type,
        house_model,

        ---------- numerics
        cast(arrival_book_value as numeric) as arrival_book_value,

        ---------- booleans
        migrated as is_migrated,

        ---------- dates
        cast(opened_date as date) as opened_date

    from source

)

select * from renamed
