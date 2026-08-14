with source as (

    select * from {{ source('generated', 'gen_desk') }}

),

renamed as (

    select

        ---------- ids
        desk_id,
        head_rm_id,

        ---------- strings
        desk_name,
        region as desk_region

    from source

)

select * from renamed
