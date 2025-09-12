{% macro configure_data_masking(
    temp_data_masking_config_table_name,
    config_data_masking_table_name,
    create_temp_data_masking_config_table_query,
    create_data_masking_config_table_query,
    access_management_database_name,
    access_management_schema_name) %}
    {% do adapter.dispatch('configure_data_masking')(
        temp_data_masking_config_table_name,
        config_data_masking_table_name,
        create_temp_data_masking_config_table_query,
        create_data_masking_config_table_query,
        access_management_database_name,
        access_management_schema_name) %}
{% endmacro %}
