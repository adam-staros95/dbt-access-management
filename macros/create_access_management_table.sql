{% macro create_access_management_table(create_access_management_table_query) %}
    {% do run_query(create_access_management_table_query) %}
{% endmacro %}
