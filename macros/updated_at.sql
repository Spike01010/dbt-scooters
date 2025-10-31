
{# 
{% macro updated_at() %}
    now() at time zone 'utc' as updated_at
{% endmacro %}
#}


{% macro updated_at() %}
    now()  as updated_at
{% endmacro %}