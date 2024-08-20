{% macro execute_grants_for_configured_entities() %}
    {% set unique_grants = [] %}

    {% set objects_in_database = get_objects_in_database() %}

    {% set query %}
        SELECT json_parse(grants::varchar) as grants
        FROM access_management.{{project_name}}_config
        WHERE database_name || '.' || schema_name || '.' || model_name || '.' ||  CASE
            WHEN lower(materialization) = 'view' THEN 'view'  ELSE 'table' END
        IN ({{ "'" ~ objects_in_database | join("', '") ~ "'" }})
    {% endset %}

    {% set grants_result = run_query(query) %}
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
{% endmacro %}
