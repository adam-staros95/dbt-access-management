{% macro execute_all_grants() %}
    {% set unique_grants = [] %}

    {% set objects_in_database = [] %}

    {% set objects_in_database_rows = run_query("SELECT table_catalog || '.' || table_schema || '.' || table_name || '.' ||  CASE WHEN lower(table_type) = 'base table' THEN 'table' ELSE 'view' END FROM information_schema.tables;") %}

    {% for row in objects_in_database_rows %}
        {% do objects_in_database.append(row[0]) %}
    {% endfor %}

    {% set query %}
        SELECT json_parse(grants::varchar)
        FROM access_management.config
        WHERE database_name || '.' || schema_name || '.' || model_name || '.' ||  CASE
            WHEN lower(materialization) = 'view' THEN 'view'  ELSE 'table' END
        IN ({{ "'" ~ objects_in_database | join("', '") ~ "'" }})
    {% endset %}

    {% set grants_result = run_query(query) %}
    {% if grants_result %}
        {% set grants_rows = grants_result %}
        {% for row in grants_rows %}
            {% set grants_list = fromjson(row[0]) %}
            {% for grant_query in grants_list %}
                {% if grant_query not in unique_grants %}
                    {% do unique_grants.append(grant_query) %}
                {% endif %}
            {% endfor %}
        {% endfor %}

        {% set grant_query = unique_grants | join(' ') %}

        {{ log(grant_query, info=True) }}
        {% set result = run_query(grant_query) %}
        {% if result is not none %}
            {% do log("Query success: " ~ unique_grant, info=True) %}
        {% else %} {% do log("Query failed: " ~ unique_grant, info=True) %}
        {% endif %}

    {% else %} {{ log("No grants found", info=True) }}
    {% endif %}
{% endmacro %}
