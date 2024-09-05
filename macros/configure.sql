{% macro configure(
    temp_access_management_config_table_name,
    config_access_management_table_name,
    create_temp_access_management_config_table_query,
    create_access_management_config_table_query,
    temp_data_masking_config_table_name,
    config_data_masking_table_name,
    create_temp_data_masking_config_table_query,
    create_data_masking_config_table_query
) %}
    {{ log("Configuring access management and data masking", info=True) }}
    {% do configure_access_management(temp_access_management_config_table_name, config_access_management_table_name, create_temp_access_management_config_table_query, create_access_management_config_table_query) %}
    {{ log("Access management configured", info=True) }}
    {% do configure_masking_policies(temp_data_masking_config_table_name, config_data_masking_table_name, create_temp_data_masking_config_table_query, create_data_masking_config_table_query) %}
    {{ log("Data masking configured", info=True) }}

{% endmacro %}
