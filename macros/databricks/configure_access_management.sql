{% macro databricks__configure_access_management(
    temp_access_management_config_table_name,
    config_access_management_table_name,
    create_temp_access_management_config_table_query,
    create_access_management_config_table_query,
    access_management_database_name,
    access_management_schema_name
    ) %}
    {{ log("Creating temporary access config table " ~ temp_access_management_config_table_name, info=True) }}
    {% do run_query(create_temp_access_management_config_table_query) %}
    {% do validate_configured_identities(database_name=access_management_database_name, schema_name=access_management_schema_name, config_table_name=temp_access_management_config_table_name, should_stop_execution=True) %}

    {% set all_databases = get_all_configured_databases(
        database_name=access_management_database_name,
        schema_name=access_management_schema_name,
        config_table_name=config_access_management_table_name,
        temp_config_table_name=temp_access_management_config_table_name
    ) %}

    {{ log("All configured databases: " ~ all_databases | join(", "), info=True) }}
    {% do run_query(create_access_management_config_table_query) %}
    {% do drop_temp_config_table(database_name=access_management_database_name, schema_name=access_management_schema_name, temp_config_table_name=temp_access_management_config_table_name) %}
{% endmacro %}

{% macro get_all_configured_databases(database_name, schema_name, config_table_name, temp_config_table_name) %}
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

    {% set combined = (databases_configured_in_config_table + databases_configured_in_temp_config_table) | unique %}
    {{ return(combined) }}
{% endmacro %}

{% macro get_configured_databases(database_name, schema_name, config_table_name, should_check_table_exists=True) %}
    {%- set relation = database_name ~ '.' ~ schema_name ~ '.' ~ config_table_name -%}
    {%- set res = [] -%}

    {% if should_check_table_exists %}
        {% set check_table_exists_query %}
            SELECT COUNT(*) as cnt
            FROM {{database_name}}.information_schema.tables
            WHERE table_schema = '{{schema_name}}'
              AND table_name = '{{config_table_name}}'
        {% endset %}

        {% set check_table_exists_result = run_query(check_table_exists_query) %}
        {% if execute %}
            {% set exists = check_table_exists_result.columns[0].values()[0] | int %}
        {% else %} {% set exists = 0 %}
        {% endif %}

        {% if exists == 0 %}
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
