with clients as (

    select * from {{ ref('stg_generated__clients') }}

),

entities as (

    select * from {{ ref('stg_gleif__entities') }}

),

relationship_managers as (

    select * from {{ ref('stg_generated__relationship_managers') }}

),

current_assignments as (

    select
        client_id,
        rm_id
    from {{ ref('stg_generated__rm_assignments') }}
    qualify row_number() over (
            partition by client_id order by assigned_date desc
        ) = 1

),

enriched as (

    select

        ---------- ids
        clients.client_id,
        clients.lei,
        current_assignments.rm_id as current_rm_id,

        ---------- strings
        clients.client_name,
        clients.client_type,
        clients.domicile_country,
        clients.risk_profile,
        relationship_managers.rm_name as current_rm_name,
        entities.legal_name,
        entities.legal_country,
        entities.entity_status,

        ---------- dates
        clients.client_since_date

    from clients
    left join entities
        on clients.lei = entities.lei
    left join current_assignments
        on clients.client_id = current_assignments.client_id
    left join relationship_managers
        on current_assignments.rm_id = relationship_managers.rm_id

)

select * from enriched
