{#
    After the base date, a NEW record must open a key the full file does not
    carry; a collision means the replay re-opened an existing instrument. The
    base's own publication day is excluded on purpose: that file legitimately
    restates base keys and the model filters it, so unscoped this test would
    fail on correct data.
#}

with new_keys as (

    select distinct
        isin,
        trading_venue_mic
    from {{ ref('stg_esma_firds__instrument_deltas') }}
    where record_type = 'NEW'
        and publication_date > date '{{ var("firds_base_date") }}'

),

base_keys as (

    select
        isin,
        trading_venue_mic
    from {{ ref('stg_esma_firds__instruments') }}

)

select
    new_keys.isin,
    new_keys.trading_venue_mic
from new_keys
inner join base_keys using (isin, trading_venue_mic)
