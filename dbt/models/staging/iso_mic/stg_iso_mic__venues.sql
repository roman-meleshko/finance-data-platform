with source as (

    select * from {{ source('iso_mic', 'iso_mic') }}

),

renamed as (

    select

        ---------- ids
        mic,
        operating_mic,
        nullif(lei, '') as lei,

        ---------- strings
        market_name_institution_description as market_name,
        nullif(legal_entity_name, '') as legal_entity_name,
        market_category_code,
        iso_country_code as country_code,
        city,
        status,
        source_file,

        ---------- booleans
        oprt_sgmt = 'OPRT' as is_operating_mic,

        ---------- dates
        safe.parse_date('%Y%m%d', creation_date) as creation_date,
        safe.parse_date('%Y%m%d', last_update_date) as last_update_date,
        safe.parse_date(
            '%Y%m%d', nullif(last_validation_date, '')
        ) as last_validation_date,
        safe.parse_date('%Y%m%d', nullif(expiry_date, '')) as expiry_date,
        safe_cast(publication_date as date) as publication_date,

        ---------- timestamps
        cast(ingested_at as timestamp) as ingested_at

    from source

)

select * from renamed
