{% macro execute_grants(access_management_database_name, access_management_schema_name) %}
    {% do adapter.dispatch('execute_grants', 'dbt_access_management')(access_management_database_name, access_management_schema_name) %}
{% endmacro %}
