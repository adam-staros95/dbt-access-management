{% macro databricks__mask_data(access_management_database_name, access_management_schema_name) %}
    {% if execute %}
        {% if config.get('materialized') != 'ephemeral' %}
            {% set model_masking_configs = dbt_access_management.get_model_masking_configs(
                access_management_database_name=access_management_database_name,
                access_management_schema_name=access_management_schema_name,
            ) %}
            {% if model_masking_configs %}
                {% set columns_info_for_model = dbt_access_management.get_columns_info_for_masking_configs(masking_configs=model_masking_configs) %}
                {% set statements_for_model_masking_configs = dbt_access_management.get_statements_for_added_or_updated_configs(
                    added_or_updated_configs=model_masking_configs,
                    columns_info=columns_info_for_model,
                    access_management_database_name=access_management_database_name,
                    access_management_schema_name=access_management_schema_name
                ) %}
                {% set query %}
                    begin
                    {{statements_for_model_masking_configs | join('\n')}}
                    end;
                {% endset %}
                {{ log("Query " ~ query, info=True) }}
                {% do run_query(query) %}
            {% endif %}
        {% endif %}
    {% endif %}
{% endmacro %}


{% macro get_model_masking_configs(access_management_database_name, access_management_schema_name) %}
    {%- set config_table_name = project_name ~ '_data_masking_config' -%}
    {%- set relation = access_management_database_name ~ '.' ~ access_management_schema_name ~ '.' ~ config_table_name -%}
    {% set masking_configs = [] %}

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
        and database_name = '{{ this.database }}' and schema_name = '{{ this.schema }}' and alias = '{{ this.name }}'
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
