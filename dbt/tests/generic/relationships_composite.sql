{% test relationships_composite(model, to, columns, to_columns=none) %}

{#
    Referential integrity on a composite key. The built-in relationships test
    takes one column, so a child whose foreign key is a pair can only be
    checked one side at a time, which asserts less than the reference table's
    own grain. Rows with a null in any key column are skipped, matching the
    built-in behaviour: an absent reference is not a broken one.
#}

    {%- set to_columns = to_columns or columns -%}

with child as (

    select {{ columns | join(', ') }}
    from {{ model }}
    where {% for column in columns -%}
        {{ column }} is not null
        {%- if not loop.last %} and {% endif %}
    {%- endfor %}

),

parent as (

    select {{ to_columns | join(', ') }}
    from {{ to }}

)

select child.*
from child
left join parent
    on {% for column in columns -%}
        child.{{ column }} = parent.{{ to_columns[loop.index0] }}
        {%- if not loop.last %} and {% endif %}
    {%- endfor %}
where parent.{{ to_columns[0] }} is null

{% endtest %}
