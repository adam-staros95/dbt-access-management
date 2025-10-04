{% macro mask_data(access_management_database_name, access_management_schema_name='access_management') %}
    {% do adapter.dispatch('mask_data', 'dbt_access_management')(access_management_database_name, access_management_schema_name) %}
{% endmacro %}
