{% macro configure_masking_policies(masking_config_temp_table_name='pii_dev', masking_config_table_name='pii_dev') %}
    {% set objects_in_database = get_objects_in_database() %}
    {% set database_identities = get_database_identities() %}
    {% set currently_applied_masking_configs = get_masking_configs_in_database() %}
    {% set new_masking_configs = get_new_masking_configs(masking_config_temp_table_name, objects_in_database) %}
    {% set new_masking_configs_in_format_of_system_table = map_new_configs_to_system_table_format(new_masking_configs) %}
    {% set log_currently_applied_masking_configs = currently_applied_masking_configs | join('\n') %}
    {{ log("Current:\n" ~ log_currently_applied_masking_configs, info=True) }}
    {% set log_new_masking_configs_in_format_of_system_table = new_masking_configs_in_format_of_system_table | join('\n') %}
    {{ log("New:\n" ~ log_new_masking_configs_in_format_of_system_table, info=True) }}


{% endmacro %}

{% macro get_masking_configs_in_database() %}
    {% set query_system_table %}
        select * from svv_attached_masking_policy;
    {% endset %}
    {% set query_system_table_result = dbt.run_query(query_system_table) %}
    {% set masking_configs = [] %}

    {% for row in query_system_table_result.rows %}
        {% do masking_configs.append({
            'schema_name': row.schema_name,
            'model_name': row.table_name,
            'column_name': fromjson(row.input_columns)[0],
            'grantee': row.grantee,
            'grantee_type': row.grantee_type
        }) %}
    {% endfor %}
    {{ return(masking_configs) }}
{% endmacro %}

{% macro get_new_masking_configs(masking_config_temp_table_name, objects_in_database) %}
    {% set query_config_table %}
        select t.schema_name, t.model_name, c.column_name, c.users_with_access, c.roles_with_access
        from access_management.{{ masking_config_temp_table_name }} as t, t.masking_config as c
        where t.database_name || '.' || t.schema_name || '.' || t.model_name || '.' ||  case
            when lower(t.materialization) = 'view' then 'view'  else 'table' end
        in ({{ "'" ~ objects_in_database | join("', '") ~ "'" }});
    {% endset %}

    {% set query_config_table_result = dbt.run_query(query_config_table) %}
    {% set masking_configs = [] %}

    {% for row in query_config_table_result.rows %}
        {% do masking_configs.append({
            'schema_name': row.schema_name,
            'model_name': row.model_name,
            'column_name': row.column_name,
            'users_with_access': row.users_with_access,
            'roles_with_access': row.roles_with_access
        }) %}
    {% endfor %}
    {{ return(masking_configs) }}
{% endmacro %}

{% macro map_new_configs_to_system_table_format(new_masking_configs) %}
    {% set mapped_masking_configs = [] %}
    {% for config in new_masking_configs %}
        {% for user_with_access in fromjson(config.users_with_access) %}
            {% do mapped_masking_configs.append({
                'schema_name': config.schema_name,
                'model_name': config.model_name,
                'column_name': fromjson(config.column_name),
                'grantee': user_with_access,
                'grantee_type': 'user'
            }) %}
        {% endfor %}
        {% for role_with_access in fromjson(config.roles_with_access) %}
            {% do mapped_masking_configs.append({
                'schema_name': config.schema_name,
                'model_name': config.model_name,
                'column_name': fromjson(config.column_name),
                'grantee': role_with_access,
                'grantee_type': 'role'
            }) %}
        {% endfor %}
        {% do mapped_masking_configs.append({
            'schema_name': config.schema_name,
            'model_name': config.model_name,
            'column_name': fromjson(config.column_name),
            'grantee': 'public',
            'grantee_type': 'public'
        }) %}
    {% endfor %}
    {{ return(mapped_masking_configs) }}
{% endmacro %}
