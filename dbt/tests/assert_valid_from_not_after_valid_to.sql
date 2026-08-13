{#
    A singular test returns the rows that violate the invariant, so the
    predicate is its negation: intervals where the end precedes the start.
#}

with versioned as (

    select * from {{ ref('int_esma_firds__instruments_versioned') }}

)

select
    isin,
    trading_venue_mic,
    valid_from,
    valid_to
from versioned
where valid_from > valid_to
