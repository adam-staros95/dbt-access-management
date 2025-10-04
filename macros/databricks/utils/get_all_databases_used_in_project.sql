{% macro get_all_databases_used_in_project(database_name, schema_name, config_table_name, temp_config_table_name) %}
    {% set databases_configured_in_config_table = get_configured_databases(
        database_name=database_name,
        schema_name=schema_name,
        config_table_name=config_table_name
    ) %}

    {% set databases_configured_in_temp_config_table = get_configured_databases(
        database_name=database_name,
        schema_name=schema_name,
        config_table_name=temp_config_table_name,
        should_check_table_exists=False
    ) %}

    {% set combined = (databases_configured_in_config_table + databases_configured_in_temp_config_table) | unique | list %}
    {{ return(combined) }}
{% endmacro %}

{% macro get_configured_databases(database_name, schema_name, config_table_name, should_check_table_exists=True) %}
    {%- set relation = database_name ~ '.' ~ schema_name ~ '.' ~ config_table_name -%}
    {%- set res = [] -%}

    {% if should_check_table_exists %}
        {% if not check_table_exists(database_name, schema_name, config_table_name) %}
            {{ log("Table " ~ relation ~ " does not exist yet.", info=True) }}
            {{ return([]) }}
        {% endif %}
    {% endif %}

    {% set query_config_table %}
        select distinct database_name
        from {{ relation }};
    {% endset %}

    {% set query_result = run_query(query_config_table) %}

    {% if execute %} {% set res = query_result.columns[0].values() %} {% endif %}

    {{ return(res | list) }}
{% endmacro %}
