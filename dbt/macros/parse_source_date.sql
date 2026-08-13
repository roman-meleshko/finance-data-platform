{% macro parse_source_date(column) %}
    case
        when substr({{ column }}, 1, 4) >= '9990' then null
        when substr({{ column }}, 1, 4) <= '1900' then null
        else safe_cast(substr({{ column }}, 1, 10) as date)
    end
{% endmacro %}
