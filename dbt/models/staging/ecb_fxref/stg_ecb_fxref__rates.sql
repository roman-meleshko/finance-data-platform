with source as (

    select * from {{ source('ecb_fxref', 'ecb_fxref') }}

),

renamed as (

    select

        ---------- strings
        currency,
        source_file,

        ---------- numerics
        safe_cast(fx_rate as numeric) as units_per_eur,

        ---------- dates
        safe_cast(date as date) as rate_date,
        safe_cast(publication_date as date) as publication_date,

        ---------- timestamps
        cast(ingested_at as timestamp) as ingested_at

    from source

)

select * from renamed
