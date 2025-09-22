-- TODO: Move to helpers
{% macro check_table_exists(database_name, schema_name, table_name) %}
    {% set query %}
        SELECT COUNT(*) as cnt
        FROM {{database_name}}.information_schema.tables
        WHERE table_schema = '{{schema_name}}'
          AND table_name = '{{table_name}}'
    {% endset %}

    {% set result = run_query(query) %}
    {% if execute %} {% set exists = result.columns[0].values()[0] | int %}
    {% else %} {% set exists = 0 %}
    {% endif %}

    {{ return(exists > 0) }}
{% endmacro %}
