{% macro apply_masking_policies_for_model() %}
    {% if execute %}
        {% if config.get('materialized') != 'ephemeral' %}
            {% if not is_incremental() %}

                {% set policy_names = dbt_access_management.get_policy_names_constants() %}

                {% set database_identities = dbt_access_management.get_database_identities() %}
                {% set users_identities = dbt_access_management.get_users(database_identities) %}
                {% set roles_identities = dbt_access_management.get_roles(database_identities) %}
                {% set masking_configs =  dbt_access_management.get_masking_configs_for_model() %}

                {% set columns = adapter.get_columns_in_relation(this) %}

                {% set configure_masking_query -%}
            {%- for masking_config in masking_configs -%}
                {%- for col in columns -%}
                    {%- if col.quoted == masking_config['column_name'] -%}
--                    TODO: Do not use dbt build in functions, add function "Get masking policy for column type"
                        {%- if col.is_string() -%}
                            ATTACH MASKING POLICY {{ policy_names.MASK_VARCHAR }}
                            ON {{ this.schema }}.{{ this.name }}({{ masking_config['column_name'] }})
                            TO PUBLIC;
                            {{ '\n' -}}
                            {%- for user in fromjson(masking_config['users_with_access']) -%}
                            {%- if user in users_identities -%}
                            ATTACH MASKING POLICY {{ policy_names.UNMASK_VARCHAR }}
                            ON {{ this.schema }}.{{ this.name }}({{ masking_config['column_name'] }})
                            TO "{{ user }}" PRIORITY 10;
                            {{ '\n' -}}
                            {%- endif -%}
                            {%- endfor -%}
                            {%- for role in fromjson(masking_config['roles_with_access']) -%}
                            {%- if role in roles_identities -%}
                            ATTACH MASKING POLICY {{ policy_names.UNMASK_VARCHAR }}
                            ON {{ this.schema }}.{{ this.name }}({{ masking_config['column_name'] }})
                            TO ROLE "{{ role }}" PRIORITY 10;
                            {{ '\n' -}}
                            {%- endif -%}
                            {%- endfor -%}
                        {%- endif -%}
                    {%- endif -%}
                {%- endfor -%}
            {%- endfor -%}
                {%- endset %}
                {{ log(configure_masking_query, info=True) }}
                {% do dbt.run_query(configure_masking_query) %}

            {% else %}
                {{ log("Applying masking policies for incremental runs is not currently supported!", info=True) }}
            {% endif %}
        {% else %}
            {{ log("Skipping assigning masking policies for ephemeral model", info=False) }}
        {% endif %}
    {% endif %}
{% endmacro %}

{% macro get_users(database_identities) %}
    {% set users = [] %}
    {% for identity in database_identities %}
        {% if identity['identity_type'] == 'user' %}
            {% do users.append(identity['identity_name']) %}
        {% endif %}
    {% endfor %}
    {{ return(users) }}
{% endmacro %}

{% macro get_roles(database_identities) %}
    {% set roles = [] %}
    {% for identity in database_identities %}
        {% if identity['identity_type'] == 'role' %}
            {% do roles.append(identity['identity_name']) %}
        {% endif %}
    {% endfor %}
    {{ return(roles) }}
{% endmacro %}

{% macro get_masking_configs_for_model() %}
    {% set query_config_table %}
        select c.column_name, c.users_with_access, c.roles_with_access from access_management.pii_dev  as t, t.masking_config as c
        where schema_name = '{{ this.schema }}' and model_name = '{{ this.name }}';
    {% endset %}

    {% set query_config_table_result = dbt.run_query(query_config_table) %}
    {% set masking_config = [] %}

    {% for row in query_config_table_result.rows %}
        {% do masking_config.append({
            'column_name': row.column_name,
            'users_with_access': row.users_with_access,
            'roles_with_access': row.roles_with_access
        }) %}
    {% endfor %}
    {{ return(masking_config) }}
{% endmacro %}
