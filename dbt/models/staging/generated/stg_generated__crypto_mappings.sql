with source as (

    select * from {{ source('generated', 'gen_crypto_mapping') }}

),

renamed as (

    select

        ---------- ids
        isin,

        ---------- strings
        underlying_symbol,
        instrument_name,
        asset_class,
        match_basis

    from source

)

select * from renamed
