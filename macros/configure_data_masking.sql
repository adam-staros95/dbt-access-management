{% macro configure_data_masking(temp_data_masking_config_table_name, config_data_masking_table_name, create_temp_data_masking_config_table_query, create_data_masking_config_table_query) %}
    {% set detach_policies_query = '' %}
    {% set attach_policies_query = '' %}

    {{ log("Creating temporary dynamic data masking config table " ~ temp_data_masking_config_table_name, info=True) }}
    {% do run_query(create_temp_data_masking_config_table_query) %}

    {% do create_project_related_masking_policies() %}
    {% set objects_in_database = get_objects_in_database() %}
    {% set database_identities = get_database_identities() %}
    {% set currently_applied_masking_configs = get_currently_applied_masking_configs_for_objects_from_new_config(temp_data_masking_config_table_name) %}
    {% set new_masking_configs = get_new_masking_configs(temp_data_masking_config_table_name, objects_in_database) %}
    {% set new_masking_configs_in_format_of_system_table = map_new_configs_to_system_table_format(new_masking_configs) %}
    {% set policies_to_detach = get_policies_to_detach(currently_applied_masking_configs, new_masking_configs_in_format_of_system_table) %}
    {% set policies_to_attach = get_policies_to_attach(currently_applied_masking_configs, new_masking_configs_in_format_of_system_table) %}
    {% do validate_configured_data_masking_identities(policies_to_attach, database_identities) %}
    {% if policies_to_detach | length > 0 %}
        {% set detach_policies_query = get_detach_policies_query(policies_to_detach) %}
    {% endif %}
    {% if policies_to_attach | length > 0 %}
        {% set table_column_types = get_table_column_types(policies_to_attach) %}
        {% set attach_policies_query = get_attach_policies_query(policies_to_attach, table_column_types) %}
    {% endif %}
    {% if detach_policies_query | trim | length > 0 or attach_policies_query | trim | length > 0 %}
        {% set configuration_query %}
            -- Detach masking policies
            {{detach_policies_query}}
            -- Attach masking policies
            {{attach_policies_query}}
        {% endset %}
        {{ log("Configure masking policies query: \n" ~ configuration_query, info=True) }}
        {% do run_query(configuration_query) %}
    {% endif %}

    {% set drop_temp_config_data_masking_table_query %}
        DROP TABLE access_management.{{temp_data_masking_config_table_name}};
    {% endset %}

    {% do run_query(create_data_masking_config_table_query) %}
    {{ log(drop_temp_config_data_masking_table_query, info=True) }}
    {% do run_query(drop_temp_config_data_masking_table_query) %}
{% endmacro %}

{% macro get_currently_applied_masking_configs_for_objects_from_new_config(temp_data_masking_config_table_name) %}
    {% set query_temp_config_table %}
        select schema_name, model_name from access_management.{{temp_data_masking_config_table_name}};
    {% endset %}

    {% set query_temp_config_table_result = dbt.run_query(query_temp_config_table) %}
    {% set temp_config_schema_table_conditions = [] %}

    {% for row in query_temp_config_table_result.rows %}
        {% do temp_config_schema_table_conditions.append(row.schema_name ~ '.' ~ row.model_name) %}
    {% endfor %}

    {% set query_system_table %}
        select * from svv_attached_masking_policy
        where
        (schema_name || '.' || table_name) in ({{ "'" ~ temp_config_schema_table_conditions | unique | join("', '") ~ "'" }});
    {% endset %}

    {% set query_system_table_result = dbt.run_query(query_system_table) %}
    {% set masking_configs = [] %}

    {% for row in query_system_table_result.rows %}
        {% do masking_configs.append({
            'schema_name': row.schema_name,
            'model_name': row.table_name,
            'column_name': fromjson(row.input_columns)[0],
            'grantee': row.grantee,
            'grantee_type': row.grantee_type,
            'policy_name': row.policy_name
        }) %}
    {% endfor %}
    {{ return(masking_configs) }}
{% endmacro %}

{% macro get_new_masking_configs(temp_data_masking_config_table_name, objects_in_database) %}
    {% set query_config_table %}
        select t.schema_name, t.model_name, c.column_name, c.users_with_access, c.roles_with_access
        from access_management.{{ temp_data_masking_config_table_name }} as t, t.masking_config as c
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


{% macro get_policies_to_detach(currently_applied_masking_configs, new_masking_configs_in_format_of_system_table) %}
    {% set removed_masking_configs = [] %}

    {% for old_item in currently_applied_masking_configs %}
        {% if {
            'schema_name': old_item.schema_name,
            'model_name': old_item.model_name,
            'column_name': old_item.column_name,
            'grantee': old_item.grantee,
            'grantee_type': old_item.grantee_type
        } not in new_masking_configs_in_format_of_system_table %}
            {% do removed_masking_configs.append(old_item) %}
        {% endif %}
    {% endfor %}

    {% do return(removed_masking_configs) %}
{% endmacro %}


{% macro get_policies_to_attach(currently_applied_masking_configs, new_masking_configs_in_format_of_system_table) %}
    {% set new_masking_configs = [] %}

    {% set currently_applied_masking_configs_without_policy_name = [] %}
    {% for old_item in currently_applied_masking_configs %}
        {% do currently_applied_masking_configs_without_policy_name.append(
        {
            'schema_name': old_item.schema_name,
            'model_name': old_item.model_name,
            'column_name': old_item.column_name,
            'grantee': old_item.grantee,
            'grantee_type': old_item.grantee_type
        }
    ) %}
    {% endfor %}
    {% for new_item in new_masking_configs_in_format_of_system_table %}
        {% if new_item not in currently_applied_masking_configs_without_policy_name %}
            {% do new_masking_configs.append(new_item) %}
        {% endif %}
    {% endfor %}

    {% do return(new_masking_configs) %}
{% endmacro %}

{% macro get_detach_policies_query(policies_to_detach) %}
    {% set query %}
    {%- for policy_to_detach in policies_to_detach -%}
    detach masking policy {{policy_to_detach['policy_name']}} on {{ policy_to_detach['schema_name'] }}.{{ policy_to_detach['model_name'] }} ({{ policy_to_detach['column_name'] }})
        {% if policy_to_detach['grantee'] == 'public' %}
            from public;
        {% elif policy_to_detach['grantee_type'] == 'role' %}
            from {{ policy_to_detach['grantee_type'] }} "{{policy_to_detach['grantee']}}";
        {% else %}
            from "{{policy_to_detach['grantee']}}";
        {% endif %}
    {% endfor %}
    {% endset %}
    {% do return(query) %}
{% endmacro %}

{% macro get_table_column_types(policies_to_attach) %}
    {% set schema_table_conditions = [] %}
    {% for policy in policies_to_attach %}
        {% do schema_table_conditions.append(policy['schema_name'] ~ "." ~ policy['model_name']) %}
    {% endfor %}

    {% set query_information_schema_table %}
        select table_schema, table_name, column_name, data_type
        from svv_columns
        where table_catalog = current_database()
        and (table_schema || '.' || table_name) in ({{ "'" ~ schema_table_conditions | unique | join("', '") ~ "'" }})
    {% endset %}

    {% set table_column_types = [] %}
    {% set query_information_schema_table_result = run_query(query_information_schema_table) %}
    {% for row in query_information_schema_table_result.rows %}
        {% do table_column_types.append(
        {
            'schema_name': row.table_schema,
            'model_name': row.table_name,
            'column_name': row.column_name,
            'data_type': row.data_type
        }
    ) %}
    {% endfor %}
    {% do return(table_column_types) %}
{% endmacro %}

{% macro validate_configured_data_masking_identities(policies_to_attach, database_identities) %}
    {% set users_identities = dbt_access_management.get_users(database_identities) %}
    {% set roles_identities = dbt_access_management.get_roles(database_identities) %}
    {% set issues = [] %}
    {% for policy_to_attach in policies_to_attach %}
        {% if policy_to_attach['grantee_type'] != 'public' and (
            (policy_to_attach['grantee_type'] == 'user' and policy_to_attach['grantee'] not in users_identities) or
            (policy_to_attach['grantee_type'] == 'role' and policy_to_attach['grantee'] not in roles_identities)
        ) %}
            {% do issues.append(policy_to_attach['grantee_type'] ~ ' : ' ~ policy_to_attach['grantee']) %}
        {% endif %}
    {% endfor %}
    {% if issues | length > 0 %}
        {% set issue_message = "The following identities configured in DBT access management data masking, but do not exist in database:\n" ~ issues | unique | join(', ') %}
        {{ exceptions.raise_compiler_error(issue_message) }}
    {% endif %}
{% endmacro %}

{% macro get_attach_policies_query(policies_to_attach, table_column_types) %}
    {% set issues = [] %}
    {%- set configure_masking_query -%}
    {%- for policy_to_attach in policies_to_attach -%}
        {% set ns = namespace (data_type = None) %}
        {%- for table_column_type in table_column_types -%}
            {% if policy_to_attach['schema_name'] == table_column_type['schema_name'] and
                  policy_to_attach['model_name'] == table_column_type['model_name'] and
                  policy_to_attach['column_name'] == table_column_type['column_name'] %}

                {% set ns.data_type = table_column_type['data_type'] %}
                {% set masking_policies = dbt_access_management.get_masking_policy_for_data_type(policy_to_attach['column_name'], ns.data_type) %}

                {%- if masking_policies['masking_policy'] is not none and masking_policies['unmasking_policy'] is not none -%}
                    {%- if policy_to_attach['grantee_type'] == 'public' -%}
                        ATTACH MASKING POLICY {{ masking_policies['masking_policy'] }}
                        ON {{ policy_to_attach['schema_name'] }}.{{ policy_to_attach['model_name'] }}({{ policy_to_attach['column_name'] }})
                        TO PUBLIC;
                    {%- elif policy_to_attach['grantee_type'] == 'role' -%}
                        ATTACH MASKING POLICY {{ masking_policies['unmasking_policy'] }}
                        ON {{ policy_to_attach['schema_name'] }}.{{ policy_to_attach['model_name'] }}({{ policy_to_attach['column_name'] }})
                        TO ROLE "{{ policy_to_attach['grantee'] }}" PRIORITY 10;
                    {%- else -%}
                        ATTACH MASKING POLICY {{ masking_policies['unmasking_policy'] }}
                        ON {{ policy_to_attach['schema_name'] }}.{{ policy_to_attach['model_name'] }}({{ policy_to_attach['column_name'] }})
                        TO "{{ policy_to_attach['grantee'] }}" PRIORITY 10;
                    {%- endif -%}
                {% endif %}
                {% break %}
            {% endif %}
        {%- endfor -%}
        {% if ns.data_type is none %}
            {% do issues.append(policy_to_attach['schema_name'] ~ '.' ~ policy_to_attach['model_name'] ~ '.' ~ policy_to_attach['column_name']) %}
        {% endif %}
    {%- endfor -%}
    {%- endset -%}

    {% if issues | length > 0 %}
        {% set issue_message = "You configured data masking for following columns which don't exist yet:\n" ~ issues | unique | join(', ') %}
        {{ log(issue_message, info=True) }}
    {% endif %}

    {% do return(configure_masking_query) %}
{% endmacro %}
