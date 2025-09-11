{% macro databricks__configure_access_management(temp_access_management_config_table_name, config_access_management_table_name, create_temp_access_management_config_table_query, create_access_management_config_table_query) %}
    {{ log("Creating temporary access config table " ~ temp_access_management_config_table_name, info=True) }}
    {% do run_query(create_temp_access_management_config_table_query) %}
{% endmacro %}
