with accounts as (

    select * from {{ ref('stg_generated__accounts') }}

),

clients as (

    select * from {{ ref('stg_generated__clients') }}

),

desks as (

    select * from {{ ref('stg_generated__desks') }}

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

accounts_wide as (

    select

        ---------- ids
        accounts.account_id,
        accounts.client_id,
        accounts.desk_id,
        accounts.rm_id as opening_rm_id,
        current_assignments.rm_id as current_rm_id,

        ---------- strings
        clients.client_name,
        clients.client_type,
        clients.domicile_country as client_domicile,
        clients.risk_profile as client_risk_profile,
        relationship_managers.rm_name as current_rm_name,
        desks.desk_name,
        desks.desk_region,
        accounts.base_currency,
        accounts.mandate_type,
        accounts.house_model,

        ---------- numerics
        accounts.arrival_book_value,

        ---------- booleans
        accounts.is_migrated,

        ---------- dates
        accounts.opened_date

    from accounts
    left join clients
        on accounts.client_id = clients.client_id
    left join desks
        on accounts.desk_id = desks.desk_id
    left join current_assignments
        on accounts.client_id = current_assignments.client_id
    left join relationship_managers
        on current_assignments.rm_id = relationship_managers.rm_id

)

select * from accounts_wide
