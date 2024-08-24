--TODO: REFACTOR TO USE get_database_identities

{% macro execute_grants_for_model() %}
    {% if execute %}
        {% if config.get('materialized') != 'ephemeral' %}
            {% set database_identities = dbt_access_management.get_database_identities() %}

            {% set identity_conditions = [] %}

            {% for identity in database_identities %}
                {% do identity_conditions.append("('" ~ identity['identity_type'] ~ "', '" ~ identity['identity_name'] ~ "')") %}
            {% endfor %}

            {% if database_identities | length > 0 %}
                {% set identities_in_clause = identity_conditions | join(", ") %}
                {% set query_config_table %}
            SELECT json_parse(grants::varchar) AS grants
            FROM access_management.{{project_name}}_config
            WHERE schema_name = '{{ this.schema }}'
            AND model_name = '{{ this.name }}'
            AND (identity_type, identity_name) IN ({{ identities_in_clause }});
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

                {% if all_grants_for_a_model %}
                    {% set all_grants_query = all_grants_for_a_model | join('\n') %}
                    {% set log_message = 'Executing grants:\n' ~ all_grants_query ~ '\n for ' ~ this.schema ~ '.' ~ this.name %}
                    {{ log(log_message, info=True) }}
                    {% do dbt.run_query(all_grants_query) %}
                {% else %} {{ log("No grants to execute.", info=True) }}
                {% endif %}
            {% else %}
                {{ log("No identities found; skipping grants execution.", info=True) }}
            {% endif %}

        {% else %}
            {{ log("Skipping assigning permissions for ephemeral model", info=False) }}
        {% endif %}
    {% endif %}
{% endmacro %}
