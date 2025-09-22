{% macro execute_grants(am_database_name, am_schema_name) %}
    {% do adapter.dispatch('execute_grants', 'dbt_access_management')(am_database_name, am_schema_name) %}
{% endmacro %}
