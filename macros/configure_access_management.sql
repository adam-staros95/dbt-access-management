{% macro configure_access_management(
    temp_access_management_config_table_name,
    config_access_management_table_name,
    create_temp_access_management_config_table_query,
    create_access_management_config_table_query,
    access_management_database_name,
    access_management_schema_name) %}
    {% do adapter.dispatch('configure_access_management')(
        temp_access_management_config_table_name,
        config_access_management_table_name,
        create_temp_access_management_config_table_query,
        create_access_management_config_table_query,
        access_management_database_name,
        access_management_schema_name) %}
{% endmacro %}
