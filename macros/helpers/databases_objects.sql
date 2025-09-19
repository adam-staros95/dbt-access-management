{% macro get_objects_in_databases(databases=None) %}
    {{ adapter.dispatch('get_objects_in_databases')(databases) }}
{% endmacro %}

-- TODO: Add redshift support for multiple databases; Implement iteration over databases
{% macro redshift__get_objects_in_databases(databases) %}
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
            {% do objects_in_database.append(row.full_table_name) %}
        {% endfor %}
    {% endif %}
    {{ return(objects_in_database) }}
{% endmacro %}

{% macro databricks__get_objects_in_databases(databases) %}
    {% set objects_in_database = [] %}
    {% set queries = [] %}
    {% for database in databases %}
        {% set q %}
            select
                table_catalog || '.' || table_schema || '.' || table_name as full_table_name
            from {{ database }}.information_schema.tables
        {% endset %}
        {% do queries.append(q) %}
    {% endfor %}

    {% set union_query = queries | join(' union all ') %}
    {% set objects_in_database_rows = run_query(union_query) %}

    {% if objects_in_database_rows %}
        {% for row in objects_in_database_rows %}
            {% do objects_in_database.append(row.full_table_name) %}
        {% endfor %}
    {% endif %}

    {{ return(objects_in_database) }}
{% endmacro %}
