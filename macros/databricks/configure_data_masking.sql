{% macro databricks__configure_data_masking(
     temp_data_masking_config_table_name,
     config_data_masking_table_name,
     create_temp_data_masking_config_table_query,
     create_data_masking_config_table_query,
     access_management_database_name,
     access_management_schema_name
    ) %}
    {{ log("Creating temporary data masking config table " ~ temp_data_masking_config_table_name, info=True) }}
    {% do run_query(create_temp_data_masking_config_table_query) %}
    -- TODO: Implement validate_configured_identities for data masking
    {% set databases_used_in_project = get_all_databases_used_in_project(
        database_name=access_management_database_name,
        schema_name=access_management_schema_name,
        config_table_name=config_data_masking_table_name,
        temp_config_table_name=temp_data_masking_config_table_name
    ) %}
    -- TODO: Check if information about materialization is required
    {% set objects_in_databases = get_objects_in_databases(databases=databases_used_in_project) %}
    {% do run_query(create_data_masking_config_table_query) %}
    {% do drop_temp_config_table(
        database_name=access_management_database_name,
        schema_name=access_management_schema_name,
        temp_config_table_name=temp_data_masking_config_table_name
    ) %}
{% endmacro %}
