{#
    Checking whether the constant (firds_base_date is stated in dbt_project.yml)
    is aligned with the current FIRDS full export (base).
#}

with snapshot as (

    select max(publication_date) as snapshot_publication_date
    from {{ ref('stg_esma_firds__instruments') }}

)

select snapshot_publication_date
from snapshot
where snapshot_publication_date != date '{{ var("firds_base_date") }}'
