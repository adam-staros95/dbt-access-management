{% macro get_objects_in_database() %}
    {% set objects_in_database = [] %}
    {% set query %}
    SELECT
        table_catalog || '.' ||
        table_schema || '.' ||
        table_name || '.' ||
        CASE
            WHEN lower(table_type) = 'base table' THEN 'table'
            ELSE 'view'
        END AS full_table_name
    FROM
        information_schema.tables;
    {% endset %}

    {% set objects_in_database_rows = run_query(query) %}
    {% if objects_in_database_rows %}
        {% for row in objects_in_database_rows %}
            {% do objects_in_database.append(row[0]) %}
        {% endfor %}
    {% endif %}
    {{ return(objects_in_database) }}
{% endmacro %}