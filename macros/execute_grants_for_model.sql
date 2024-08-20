{% macro execute_grants_for_model() %}
    {% if execute %}
        {% if config.get('materialized') != 'ephemeral' %}

            {% set query_entities %}
        SELECT usename AS entity_name, 'user' AS entity_type FROM PG_USER
        UNION ALL
        SELECT role_name AS entity_name, 'role' AS entity_type FROM SVV_ROLES
        UNION ALL
        SELECT groname AS entity_name, 'group' AS entity_type FROM PG_GROUP;
            {% endset %}

            {% set entities_result = dbt.run_query(query_entities) %}

            {% set entity_conditions = [] %}

            {% for row in entities_result.rows %}
                {% do entity_conditions.append("('" ~ row['entity_type'] ~ "', '" ~ row['entity_name'] ~ "')") %}
            {% endfor %}

            {% if entity_conditions | length > 0 %}
                {% set in_clause = entity_conditions | join(", ") %}
                {% set query_config_table %}
            SELECT json_parse(grants::varchar) AS grants
            FROM access_management.{{project_name}}_config
            WHERE schema_name = '{{ this.schema }}'
            AND model_name = '{{ this.name }}'
            AND (entity_type, entity_name) IN ({{ in_clause }});
                {% endset %}

                {% set query_config_table_result = dbt.run_query(query_config_table) %}

                {% set grant_queries = [] %}

                {% for row in query_config_table_result.rows %}
                    {% do grant_queries.append(fromjson(row['grants'])) %}
                {% endfor %}

                {% set all_grants = [] %}

                {% for grants_list in grant_queries %}
                    {% for grant in grants_list %}
                        {% do all_grants.append(grant) %}
                    {% endfor %}
                {% endfor %}

                {% if all_grants %}
                    {% set all_grants_query = all_grants | join('\n') %}
                    {% set log_message = 'Executing grants:\n' ~ all_grants_query ~ '\n for ' ~ this.schema ~ '.' ~ this.name %}
                    {{ log(log_message, info=True) }}
                    {% do dbt.run_query(all_grants_query) %}
                {% else %} {{ log("No grants to execute.", info=True) }}
                {% endif %}
            {% else %}
                {{ log("No entities found; skipping grants execution.", info=True) }}
            {% endif %}

        {% else %}
            {{ log("Skipping assigning permissions for ephemeral model", info=False) }}
        {% endif %}
    {% endif %}
{% endmacro %}
