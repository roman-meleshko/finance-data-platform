{#
    Uniqueness of (key, valid_from) is already asserted in the model's yml, so
    the remaining way intervals can be wrong is a version starting on or before
    the previous version's end. valid_to is inclusive, so a version starting on
    the same day the previous one ended is an overlap: both claim that day.
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
qualify valid_from <= prev_valid_to
