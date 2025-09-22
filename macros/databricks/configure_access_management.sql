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

    {% set databases_used_in_project = get_all_databases_used_in_project(
        database_name=access_management_database_name,
        schema_name=access_management_schema_name,
        config_table_name=config_access_management_table_name,
        temp_config_table_name=temp_access_management_config_table_name
    ) %}
    -- TODO: Check if information about materialization is required
    {% set objects_in_databases = get_objects_in_databases(databases=databases_used_in_project) %}

    {% set new_unique_grants_and_revokes = get_unique_grants_and_revokes(
        database_name=access_management_database_name,
        schema_name=access_management_schema_name,
        config_table_name=temp_access_management_config_table_name,
        objects_in_databases=objects_in_databases,
        should_check_table_exists=False
    ) %}
    {% set new_unique_grants = new_unique_grants_and_revokes['unique_grants'] %}
    {% set new_unique_revokes = new_unique_grants_and_revokes['unique_revokes'] %}

    {% set previous_unique_grants_and_revokes = get_unique_grants_and_revokes(
        database_name=access_management_database_name,
        schema_name=access_management_schema_name,
        config_table_name=config_access_management_table_name,
        objects_in_databases=objects_in_databases,
        should_check_table_exists=True
    ) %}
    {% set previous_unique_grants = previous_unique_grants_and_revokes['unique_grants'] %}
    {% set previous_unique_revokes = previous_unique_grants_and_revokes['unique_revokes'] %}

    -- TODO: Move to helpers
    {% set revokes_to_execute = get_previous_unique_revokes_which_do_not_exist_in_new_config(new_unique_revokes, previous_unique_revokes) %}
    {% set grants_to_execute = get_new_unique_grants_which_do_not_exist_in_previous_config(new_unique_grants, previous_unique_grants) %}

    {% if (revokes_to_execute | length) > 0 or (grants_to_execute | length) > 0 %}
        {% set execute_revokes_and_grants_query %}
        BEGIN
        -- Revokes
        {{revokes_to_execute | join('\n')}}
        -- Grants
        {{grants_to_execute | join('\n')}}
        END;
        {% endset %}
        {{ log("Running revokes and grants:\n" ~ execute_revokes_and_grants_query, info=True) }}
        {% do run_query(execute_revokes_and_grants_query) %}
    {% else %} {{ log("No grants or revokes to execute", info=True) }}
    {% endif %}
    {% do run_query(create_access_management_config_table_query) %}
    {% do drop_temp_config_table(database_name=access_management_database_name, schema_name=access_management_schema_name, temp_config_table_name=temp_access_management_config_table_name) %}
{% endmacro %}

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

    {% set combined = (databases_configured_in_config_table + databases_configured_in_temp_config_table) | unique %}
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

{% macro get_unique_grants_and_revokes(database_name, schema_name, config_table_name, objects_in_databases, should_check_table_exists=True) %}
    {%- set relation = database_name ~ '.' ~ schema_name ~ '.' ~ config_table_name -%}
    {% set unique_grants = [] %}
    {% set unique_revokes = [] %}

    {% if should_check_table_exists %}
        {% if not check_table_exists(database_name, schema_name, config_table_name) %}
            {{ log("Table " ~ relation ~ " does not exist yet.", info=True) }}
            {{ return({
                'unique_grants': unique_grants,
                'unique_revokes': unique_revokes
            }) }}
        {% endif %}
    {% endif %}

    {% set query_config_table %}
        select grants, revokes
        from {{ relation }}
        where database_name || '.' || schema_name || '.' || alias
        in ({{ "'" ~ objects_in_databases | join("', '") ~ "'" }})
    {% endset %}

    {% set grants_and_revokes = run_query(query_config_table) %}

    {% if grants_and_revokes %}
        {% for row in grants_and_revokes.rows %}
            {% set grants_list = fromjson(row.grants) %}
            {% set revokes_list = fromjson(row.revokes) %}
            {% for grant_query in grants_list %}
                {% if grant_query not in unique_grants %}
                    {% do unique_grants.append(grant_query) %}
                {% endif %}
            {% endfor %}
            {% for revoke_query in revokes_list %}
                {% if revoke_query not in unique_revokes %}
                    {% do unique_revokes.append(revoke_query) %}
                {% endif %}
            {% endfor %}
        {% endfor %}
        {{ return({
        'unique_grants': unique_grants,
        'unique_revokes': unique_revokes
        }) }}
    {% else %}
        {{ log("No grants and revokes found in table: " ~ relation, info=True) }}
        {% set result = {
        'unique_grants': unique_grants,
        'unique_revokes': unique_revokes
    } %}
    {% endif %}
{% endmacro %}
