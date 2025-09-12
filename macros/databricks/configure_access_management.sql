{% macro databricks__configure_access_management(
    temp_access_management_config_table_name,
    config_access_management_table_name,
    create_temp_access_management_config_table_query,
    create_access_management_config_table_query,
    access_management_database_name,
    access_management_schema_name
    ) %}
    {{ log("Creating temporary access config table " ~ temp_access_management_config_table_name, info=True) }}
    {% do run_query(create_temp_access_management_config_table_query) %}
    {% do validate_configured_identities(database_name=access_management_database_name, schema_name=access_management_schema_name, config_table_name=temp_access_management_config_table_name, should_stop_execution=True) %}
{% endmacro %}
