{% macro check_should_drop_configuration_table(schema_name, temp_configuration_table_name, configuration_table_name) %}
    {%- set configuration_table_query -%}
        select
            column_name
        from svv_columns
        where table_catalog = current_database() and table_schema = '{{ schema_name }}' and table_name = '{{ configuration_table_name }}'
    {%- endset -%}

    {%- set temp_configuration_table_query -%}
        select
            column_name
        from svv_columns
        where table_catalog = current_database() and table_schema = '{{ schema_name }}' and table_name = '{{ temp_configuration_table_name }}'
    {%- endset -%}

    {% set configuration_table_query_result = dbt.run_query(configuration_table_query) %}
    {% set temp_configuration_table_query_result = dbt.run_query(temp_configuration_table_query) %}

    {% set configuration_table_columns = [] %}
    {% set temp_configuration_table_columns = [] %}

    {% for row in configuration_table_query_result.rows %}
        {% do configuration_table_columns.append(row.column_name) %}
    {% endfor %}

    {% for row in temp_configuration_table_query_result.rows %}
        {% do temp_configuration_table_columns.append(row.column_name) %}
    {% endfor %}

    {% if configuration_table_columns | length == 0 %} {{ return(false) }} {% endif %}

    {% for temp_configuration_table_column in temp_configuration_table_columns %}
        {% if temp_configuration_table_column not in configuration_table_columns %}
            {{ return(true) }}
        {% endif %}
    {% endfor %}

    {% for configuration_table_column in configuration_table_columns %}
        {% if configuration_table_column not in temp_configuration_table_columns %}
            {{ return(true) }}
        {% endif %}
    {% endfor %}

    {{ return(false) }}
{% endmacro %}
