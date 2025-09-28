{% macro databricks__execute_grants(access_management_database_name, access_management_schema_name) %}
    {% if execute %}
        {% if config.get('materialized') != 'ephemeral' %}
            {% set principal_does_not_exist_sql_state = '42704' %}
            {% set query_config_table %}
                select grants
                from {{access_management_database_name}}.{{access_management_schema_name}}.{{project_name}}_access_management_config
                where database_name = '{{ this.database }}'
                and schema_name = '{{ this.schema }}'
                and alias = '{{ this.name }}'
            {% endset %}

            {% set query_config_table_result = dbt.run_query(query_config_table) %}

            {% set grant_queries = [] %}

            {% for row in query_config_table_result.rows %}
                {% do grant_queries.append(fromjson(row.grants)) %}
            {% endfor %}

            {% set all_grants_for_a_model = [] %}

            {% for grants_list in grant_queries %}
                {% for grant in grants_list %}
                    {% do all_grants_for_a_model.append(grant) %}
                {% endfor %}
            {% endfor %}

            {% set unique_grants = all_grants_for_a_model | unique | list %}
            {{ log('Uniqe grants:\n' ~ unique_grants, info=true) }}

            {% if unique_grants %}
                {% set all_grants_query %}
                begin
                {% for grant in unique_grants %}
                    begin
                        declare exit handler for sqlstate '{{ principal_does_not_exist_sql_state }}'
                        begin end;
                        {{ grant }}
                    end;
                {% endfor %}
                end;
                {% endset %}

                {{ log('Executing grants:\n' ~ all_grants_query, info=true) }}
                {% do dbt.run_query(all_grants_query) %}
            {% endif %}
        {% endif %}
    {% endif %}
{% endmacro %}
