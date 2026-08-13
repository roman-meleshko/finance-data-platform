{#
    Cancellation is retroactive erasure, so a canceled key must have no rows
    at all, including the version seeded from the full file. The canceled set
    is recomputed from staging rather than reused from the model, so a change
    to the model's own erasure logic cannot make the two agree by
    construction.
#}

with versioned as (

    select * from {{ ref('int_esma_firds__instruments_versioned') }}

),

canceled as (

    select
        isin,
        trading_venue_mic
    from {{ ref('stg_esma_firds__instrument_deltas') }}
    where record_type = 'CANC'

)

select
    versioned.isin,
    versioned.trading_venue_mic,
    versioned.version_source,
    versioned.valid_from
from versioned
inner join canceled using (isin, trading_venue_mic)
