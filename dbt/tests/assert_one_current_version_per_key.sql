{#
    Every key must have exactly one open version. Counting the current rows
    inside the group rather than filtering to them first is deliberate: a
    filter drops keys with no current version entirely, so they could never
    reach the group and the half of the invariant that catches a lost open
    interval would be untestable.
#}

with versioned as (

    select * from {{ ref('int_esma_firds__instruments_versioned') }}

)

select
    isin,
    trading_venue_mic,
    countif(is_current) as current_versions
from versioned
group by isin, trading_venue_mic
having current_versions != 1
