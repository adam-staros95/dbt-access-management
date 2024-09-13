{% macro apply_masking_policies_for_model() %}
    {% if execute %}
        {% if config.get('materialized') == 'ephemeral' %}
             {{ log("Skipping attaching masking policies for " ~ this.name ~ " ephemeral model", info=True) }}
        {% elif config.get('materialized') in ['table', 'view'] or ('incremental' in config.get('materialized') and flags.FULL_REFRESH)
            or (config.get('materialized') == 'seed' and flags.FULL_REFRESH) %}
            {% set database_identities = dbt_access_management.get_database_identities() %}
            {% set users_identities = dbt_access_management.get_users(database_identities) %}
            {% set roles_identities = dbt_access_management.get_roles(database_identities) %}
            {% set masking_configs =  dbt_access_management.get_masking_configs_for_model() %}

            {% set columns = adapter.get_columns_in_relation(this) %}

            {% set configure_masking_query -%}
                {%- for masking_config in masking_configs -%}
                    {%- for col in columns -%}
                        {%- if col.quoted == masking_config['column_name'] -%}
                            {% set masking_policies = dbt_access_management.get_masking_policy_for_data_type(col.name, col.data_type) %}
                            {%- if masking_policies['masking_policy'] is not none and masking_policies['unmasking_policy'] is not none -%}
                                ATTACH MASKING POLICY {{ masking_policies['masking_policy'] }}
                                ON {{ this.schema }}.{{ this.name }}({{ masking_config['column_name'] }})
                                TO PUBLIC;
                                {{ '\n' -}}
                                {%- for user in fromjson(masking_config['users_with_access']) -%}
                                {%- if user in users_identities -%}
                                ATTACH MASKING POLICY {{ masking_policies['unmasking_policy'] }}
                                ON {{ this.schema }}.{{ this.name }}({{ masking_config['column_name'] }})
                                TO "{{ user }}" PRIORITY 10;
                                {{ '\n' -}}
                                {%- endif -%}
                                {%- endfor -%}
                                {%- for role in fromjson(masking_config['roles_with_access']) -%}
                                {%- if role in roles_identities -%}
                                ATTACH MASKING POLICY {{ masking_policies['unmasking_policy'] }}
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
            {% if configure_masking_query %}
                {{ log(configure_masking_query, info=True) }}
                {% do dbt.run_query(configure_masking_query) %}
            {% else %}
                 {{ log("No masking configured for " ~ this.schema ~ "." ~ this.name, info=True) }}
            {% endif %}
        {% else %}
            {{ log("Skipping assigning masking policies for incremental run", info=False) }}
        {% endif %}
    {% endif %}
{% endmacro %}

{% macro get_masking_configs_for_model() %}
    {% set query_config_table %}
        select c.column_name, c.users_with_access, c.roles_with_access from access_management.{{project_name}}_data_masking_config  as t, t.masking_config as c
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
