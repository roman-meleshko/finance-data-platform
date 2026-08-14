{% test is_trading_date(model, column_name) %}

{#
    A date column must land on a known trading day. Dates absent from the
    calendar entirely count as violations too: a left-join miss returns null
    for is_trading_day, and "is false" alone would wave those through.
#}

select {{ column_name }}
from {{ model }} as facts
left join {{ ref('dim_dates') }} as calendar_days
    on facts.{{ column_name }} = calendar_days.date_key
where coalesce(calendar_days.is_trading_day, false) = false

{% endtest %}
