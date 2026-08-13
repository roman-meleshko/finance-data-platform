{{ config(severity = 'warn') }}

{#
    ESMA publishes every calendar day, weekends included, so the delta chain
    must cover every date between its first and last day. A missing day is a
    permanently wrong history for every key that changed on it and not since.
    Warn rather than error because a publisher outage is not a pipeline
    failure, but it has to be seen and ruled on.
#}

with pub_dates as (

    select distinct publication_date
    from {{ ref('stg_esma_firds__instrument_deltas') }}

),

calendar as (

    select expected_date
    from unnest(generate_date_array(
            (select min(publication_date) from pub_dates),
            (select max(publication_date) from pub_dates)
        )) as expected_date

)

select calendar.expected_date as missing_publication_date
from calendar
left join pub_dates on calendar.expected_date = pub_dates.publication_date
where pub_dates.publication_date is null
