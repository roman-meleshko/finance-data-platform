with source as (

    select * from {{ source('generated', 'gen_rm_assignment') }}

),

renamed as (

    select

        ---------- ids
        client_id,
        rm_id,

        ---------- dates
        cast(assigned_date as date) as assigned_date

    from source

)

select * from renamed
