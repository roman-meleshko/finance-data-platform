with source as (

    select * from {{ source('gleif', 'gleif_entity') }}

),

renamed as (

    select

        ---------- ids
        lei,
        nullif(successor_lei, '') as successor_lei,
        managing_lou,

        ---------- strings
        legal_name,
        legal_country,
        legal_city,
        hq_country,
        hq_city,
        nullif(legal_jurisdiction, '') as legal_jurisdiction,
        legal_form_code,
        entity_category,
        entity_status,
        registration_status,
        nullif(conformity_flag, '') as conformity_flag,
        source_file,

        ---------- dates
        safe_cast(publication_date as date) as publication_date,

        ---------- timestamps
        safe_cast(
            nullif(entity_creation_date, '') as timestamp
        ) as entity_created_at,
        safe_cast(initial_registration_date as timestamp)
            as initially_registered_at,
        safe_cast(last_update_date as timestamp) as last_updated_at,
        safe_cast(next_renewal_date as timestamp) as next_renewal_at,
        cast(ingested_at as timestamp) as ingested_at

    from source

)

select * from renamed
