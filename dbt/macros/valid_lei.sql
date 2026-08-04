{% test valid_lei(model, column_name, allow_blank=true) %}

select {{ column_name }}
from {{ model }}
where {% if allow_blank %}coalesce({{ column_name }}, '') != ''
      and {% endif %}not regexp_contains({{ column_name }}, r'^[A-Z0-9]{18}[0-9]{2}$')

{% endtest %}