with source as (

    select * from {{ source('generated', 'gen_rm') }}

),

renamed as (

    select

        ---------- ids
        rm_id,
        desk_id,

        ---------- strings
        rm_name,
        title

    from source

)

select * from renamed
