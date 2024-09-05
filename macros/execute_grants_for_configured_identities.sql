{% macro execute_grants_for_configured_identities() %}
    {% set objects_in_database = get_objects_in_database() %}

    {% set database_identities = get_database_identities() %}

    {% set identity_conditions = [] %}

    {% for identity in database_identities %}
        {% do identity_conditions.append("('" ~ identity['identity_type'] ~ "', '" ~ identity['identity_name'] ~ "')") %}
    {% endfor %}

    {% if database_identities | length > 0 %}
        {% set identities_in_clause = identity_conditions | join(", ") %}
        {% set query_config_table %}
        SELECT json_parse(grants::varchar) as grants
        FROM access_management.{{project_name}}_access_management_config
        WHERE database_name || '.' || schema_name || '.' || model_name || '.' ||  CASE
            WHEN lower(materialization) = 'view' THEN 'view'  ELSE 'table' END
        IN ({{ "'" ~ objects_in_database | join("', '") ~ "'" }})
        AND (identity_type, identity_name) IN ({{ identities_in_clause }});
        {% endset %}

        {% set unique_grants = [] %}

        {% set grants_result = run_query(query_config_table) %}
        {% if grants_result %}
            {% for row in grants_result.rows %}
                {% set grants_list = fromjson(row.grants) %}
                {% for grant_query in grants_list %}
                    {% if grant_query not in unique_grants %}
                        {% do unique_grants.append(grant_query) %}
                    {% endif %}
                {% endfor %}
            {% endfor %}

            {% set grant_query = unique_grants | join('\n') %}

            {{ log(grant_query, info=True) }}
            {% set result = run_query(grant_query) %}
            {% if result is not none %}
                {% do log("Query success:\n" ~ grant_query, info=True) %}
            {% else %} {% do log("Query failed:\n" ~ grant_query, info=True) %}
            {% endif %}

        {% else %} {{ log("No grants found", info=True) }}
        {% endif %}
    {% else %} {{ log("No identities found; skipping grants execution.", info=True) }}
    {% endif %}

{% endmacro %}
