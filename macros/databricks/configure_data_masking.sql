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

    {% set diff = diff_masking_configs(new_masking_configs=new_masking_configs, previous_masking_configs=previous_masking_configs) %}

    {% set columns_info_for_previous_masking_configs = get_columns_info_for_masking_configs(masking_configs=previous_masking_configs) %}
    {% set columns_info_for_new_masking_configs = get_columns_info_for_masking_configs(masking_configs=new_masking_configs) %}

    {% set statements_for_deleted_configs = get_statements_for_deleted_configs(
        deleted_configs=diff['deleted'],
        columns_info=columns_info_for_previous_masking_configs,
        access_management_database_name=access_management_database_name,
        access_management_schema_name=access_management_schema_name
    ) %}
    {% set statements_for_added_or_updated_configs = get_statements_for_added_or_updated_configs(
        added_or_updated_configs=diff['added_or_updated'],
        columns_info=columns_info_for_new_masking_configs,
        access_management_database_name=access_management_database_name,
        access_management_schema_name=access_management_schema_name
    ) %}

    {% if (statements_for_deleted_configs | length) > 0 or (statements_for_added_or_updated_configs | length) > 0 %}
        {% set query %}
            BEGIN
            {{statements_for_deleted_configs | join('\n')}}
            {{statements_for_added_or_updated_configs | join('\n')}}
            END;
        {% endset %}
        {{ log("Query " ~ query, info=True) }}
        {% do run_query(query) %}
    {% else %} {{ log("No changes in masking configs", info=True) }}
    {% endif %}

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
          materialization,
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
        {% set users_str = row.users_with_access %}
        {% set groups_str = row.groups_with_access %}
        {% set users_list = users_str.strip("[]").replace("'", "").split(",") %}
        {% set groups_list = groups_str.strip("[]").replace("'", "").split(",") %}

        {% do masking_configs.append({
            'database_name': row.database_name,
            'schema_name': row.schema_name,
            'alias': row.alias,
            'materialization': row.materialization,
            'column_name': row.column_name,
            'users_with_access': users_list,
            'groups_with_access': groups_list
        }) %}
    {% endfor %}
    {{ return(masking_configs) }}
{% endmacro %}

{% macro masking_key(d) %}
    {{ d['database_name'] ~ '.' ~ d['schema_name'] ~ '.' ~ d['alias'] ~ '.' ~ d['materialization'] ~ '.' ~ d['column_name'] }}
{% endmacro %}

{% macro diff_masking_configs(new_masking_configs, previous_masking_configs) %}
    {% set prev_map = {} %}
    {% for d in previous_masking_configs %}
        {% set _ = prev_map.update({ masking_key(d): d }) %}
    {% endfor %}

    {% set new_map = {} %}
    {% for d in new_masking_configs %}
        {% set _ = new_map.update({ masking_key(d): d }) %}
    {% endfor %}

    {% set deleted = [] %}
    {% for k, d in prev_map.items() %}
        {% if k not in new_map %} {% do deleted.append(d) %} {% endif %}
    {% endfor %}

    {% set added_or_updated = [] %}
    {% for k, d in new_map.items() %}
        {% if k not in prev_map or d != prev_map[k] %}
            {% do added_or_updated.append(d) %}
        {% endif %}
    {% endfor %}

    {{ return({'deleted': deleted, 'added_or_updated': added_or_updated}) }}
{% endmacro %}

{% macro get_materialization_to_securable_object_type(materialization) %}
    {% set materialization_to_securable_object_type_map = {
        "seed": "table",
        "view": "view",
        "materialized_view": "materialized view",
        "table": "table",
        "streaming_table": "table",
        "incremental": "table"
    } %}

    {{ return(materialization_to_securable_object_type_map.get(materialization | trim | lower)) }}
{% endmacro %}

{% macro generate_masking_function_name(config, access_management_database_name, access_management_schema_name) -%}
    {{- access_management_database_name ~ '.' ~
        access_management_schema_name ~ '.' ~
        config['database_name'] ~ '_' ~
        config['schema_name'] ~ '_' ~
        config['alias'] ~ '_' ~
        config['column_name'] -}}
{%- endmacro %}

{% macro get_masking_for_column_type(column_type) %}
    {% set t = column_type | lower %}
    {% if t in ['decimal', 'short', 'byte', 'int', 'long', 'double', 'float'] %}
        {{ return('NULL') }}
    {% elif t == 'boolean' %} {{ return('FALSE') }}
    {% elif t in ['date', 'timestamp'] %} {{ return('NULL') }}
    {% elif t in ['string', 'binary', 'array'] %}
        {{ return("CAST('*****' AS " ~ t ~ ")") }}
    {% else %} {{ return('NULL') }}
    {% endif %}
{% endmacro %}

{% macro get_statements_for_deleted_configs(deleted_configs, columns_info, access_management_database_name, access_management_schema_name) %}
    {% set statements = [] %}

    {% for c in deleted_configs %}
        {% set column_key = c.database_name ~ '.' ~ c.schema_name ~ '.' ~ c.alias ~ '.' ~ c.column_name %}
        {% if column_key in columns_info %}
            {%- set object_type = get_materialization_to_securable_object_type(c['materialization']) -%}
            {%- set full_table_name = c['database_name'] ~ '.' ~ c['schema_name'] ~ '.' ~ c['alias'] -%}
            {%- set alter_stmt = (
                'alter ' ~ object_type ~ ' ' ~ full_table_name ~
                ' alter column ' ~ c['column_name'] ~ ' drop mask;'
            ) -%}
            {% set drop_function_statement = 'drop function if exists ' ~ generate_masking_function_name(c, access_management_database_name, access_management_schema_name) ~ ';' %}

            {% do statements.append(alter_stmt) %}
            {% do statements.append(drop_function_statement) %}
        {% endif %}
    {% endfor %}

    {{ return(statements) }}
{% endmacro %}


{% macro get_statements_for_added_or_updated_configs(added_or_updated_configs, columns_info, access_management_database_name, access_management_schema_name) %}
    {% set statements = [] %}

    {% for c in added_or_updated_configs %}
        {% set column_key = c.database_name ~ '.' ~ c.schema_name ~ '.' ~ c.alias ~ '.' ~ c.column_name %}
        {% if column_key in columns_info %}
            {% set column_type = columns_info[column_key] %}

            {%- set function_name = generate_masking_function_name(c, access_management_database_name, access_management_schema_name) -%}

            {%- set user_conditions = [] -%}
            {%- for user in c['users_with_access'] -%}
                {%- do user_conditions.append("session_user() = '" ~ user ~ "'") -%}
            {%- endfor -%}

            {%- set group_conditions = [] -%}
            {%- for group in c['groups_with_access'] -%}
                {%- do group_conditions.append("is_account_group_member('" ~ group ~ "')") -%}
            {%- endfor -%}

            {%- set access_conditions = (group_conditions + user_conditions) | join(' OR ') -%}

            {%- set create_function_stmt = (
                'CREATE OR REPLACE FUNCTION ' ~ function_name ~ '(' ~ c['column_name'] ~ ' ' ~ column_type ~ ') RETURN CASE WHEN ' ~
                access_conditions ~ ' THEN ' ~ c['column_name'] ~ ' ELSE ' ~ get_masking_for_column_type(column_type) ~ ' END;'
            ) -%}

            {%- set object_type = get_materialization_to_securable_object_type(c['materialization']) -%}
            {%- set full_table_name = c['database_name'] ~ '.' ~ c['schema_name'] ~ '.' ~ c['alias'] -%}
            {%- set alter_table_stmt = (
                'ALTER ' ~ object_type ~ ' ' ~ full_table_name ~
                ' ALTER COLUMN ' ~ c['column_name'] ~
                ' SET MASK ' ~ function_name ~ ';'
            ) -%}

            {% do statements.append(create_function_stmt) %}
            {% do statements.append(alter_table_stmt) %}
        {% endif %}
    {% endfor %}

    {{ return(statements) }}
{% endmacro %}
