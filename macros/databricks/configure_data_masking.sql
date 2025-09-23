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
    {% set new_masking_configs = get_masking_configs(
        database_name=access_management_database_name,
        schema_name=access_management_schema_name,
        config_table_name=temp_data_masking_config_table_name,
        objects_in_databases=objects_in_databases,
        should_check_table_exists=False
    ) %}

    {% set previous_masking_configs = get_masking_configs(
        database_name=access_management_database_name,
        schema_name=access_management_schema_name,
        config_table_name=config_data_masking_table_name,
        objects_in_databases=objects_in_databases,
        should_check_table_exists=True
    ) %}

    {% do run_query(create_data_masking_config_table_query) %}
    {% do drop_temp_config_table(
        database_name=access_management_database_name,
        schema_name=access_management_schema_name,
        temp_config_table_name=temp_data_masking_config_table_name
    ) %}
{% endmacro %}

{% macro get_masking_configs(database_name, schema_name, config_table_name, objects_in_databases, should_check_table_exists) %}
    {%- set relation = database_name ~ '.' ~ schema_name ~ '.' ~ config_table_name -%}
    {% set masking_configs = [] %}

    {% if should_check_table_exists %}
        {% if not check_table_exists(database_name, schema_name, config_table_name) %}
            {{ log("Table " ~ relation ~ " does not exist yet.", info=True) }}
            {{ return(masking_configs) }}
        {% endif %}
    {% endif %}

    {% set query_config_table %}
        select
          database_name,
          schema_name,
          alias,
          col.column_name,
          col.users_with_access,
          col.groups_with_access
        from
          {{ relation }}
          lateral view
            explode(
              from_json(
                masking_config,
                'array<struct<column_name:string,users_with_access:array<string>,groups_with_access:array<string>>>'
              )
            ) as col
        where
          masking_config is not null
          and size(
            from_json(
              masking_config,
              'array<struct<column_name:string,users_with_access:array<string>,groups_with_access:array<string>>>'
            )
          ) > 0
        and database_name || '.' || schema_name || '.' || alias
        in ({{ "'" ~ objects_in_databases | join("', '") ~ "'" }})
    {% endset %}

    {% set query_config_table_result = dbt.run_query(query_config_table) %}

    {% for row in query_config_table_result.rows %}
        {% do masking_configs.append({
            'database_name': row.schema_name,
            'schema_name': row.schema_name,
            'alias': row.alias,
            'column_name': row.column_name,
            'users_with_access': row.users_with_access,
            'groups_with_access': row.groups_with_access
        }) %}
    {% endfor %}
    {{ return(masking_configs) }}
{% endmacro %}
