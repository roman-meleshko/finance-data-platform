{% macro parse_source_timestamp(column) %}
    case
        when substr({{ column }}, 1, 4) >= '9990' then null
        when substr({{ column }}, 1, 4) <= '1900' then null
        else safe_cast({{ column }} as timestamp)
    end
{% endmacro %}
