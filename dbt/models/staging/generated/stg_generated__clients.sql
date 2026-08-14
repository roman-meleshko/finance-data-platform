with source as (

    select * from {{ source('generated', 'gen_client') }}

),

renamed as (

    select

        ---------- ids
        client_id,
        lei,

        ---------- strings
        client_name,
        client_type,
        domicile_country,
        risk_profile,

        ---------- dates
        cast(client_since as date) as client_since_date

    from source

)

select * from renamed
