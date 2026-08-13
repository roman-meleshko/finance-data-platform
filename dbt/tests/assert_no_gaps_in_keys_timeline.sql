{#
    Inclusive ends mean each version starts exactly one day after the previous
    one ends, so the violations are steps of any other size. The null guard
    exempts each key's first version, which has no predecessor.
#}

with versioned as (

    select * from {{ ref('int_esma_firds__instruments_versioned') }}

)

select
    isin,
    trading_venue_mic,
    valid_from,
    lag(valid_to) over (
        partition by isin, trading_venue_mic
        order by valid_from
    ) as prev_valid_to
from versioned
qualify prev_valid_to is not null
    and date_add(prev_valid_to, interval 1 day) != valid_from
