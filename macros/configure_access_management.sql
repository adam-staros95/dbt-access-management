{% macro configure_access_management(temp_config_table_name, config_table_name, create_temp_config_table_query, create_config_table_query) %}
    {% do run_query(create_temp_config_table_query) %}
    {% do validate_configured_entities(config_table_name=temp_config_table_name, should_stop_execution=True) %}
    {% set objects_in_database = get_objects_in_database() %}
    {% set log_objects_in_database = objects_in_database | join('\n') %}
    {{ log('--------OBJECTS IN DATABASE-----------', info=True) }}
    {{ log(log_objects_in_database, info=True) }}
    {% set new_unique_grants_and_revokes = get_grants_and_revokes(objects_in_database, temp_config_table_name) %}
    {% set new_unique_grants = new_unique_grants_and_revokes['unique_grants'] %}
    {% set new_unique_revokes = new_unique_grants_and_revokes['unique_revokes'] %}
    {% set log_new_unique_grants = new_unique_grants | join('\n') %}
    {% set log_new_unique_revokes = new_unique_revokes | join('\n') %}
    {{ log('--------NEW GRANTS-----------', info=True) }}
    {{ log(log_new_unique_grants, info=True) }}
    {{ log('--------NEW REVOKES-----------', info=True) }}
    {{ log(log_new_unique_revokes, info=True) }}
    {% set previous_unique_grants_and_revokes = get_grants_and_revokes(objects_in_database, config_table_name, True) %}
    {% set previous_unique_grants = previous_unique_grants_and_revokes['unique_grants'] %}
    {% set previous_unique_revokes = previous_unique_grants_and_revokes['unique_revokes'] %}
    {% set log_previous_unique_grants = previous_unique_grants | join('\n') %}
    {% set log_previous_unique_revokes = previous_unique_revokes | join('\n') %}
    {{ log('--------PREVIOUS GRANTS-----------', info=True) }}
    {{ log(log_previous_unique_grants, info=True) }}
    {{ log('--------PREVIOUS REVOKES-----------', info=True) }}
    {{ log(log_previous_unique_revokes, info=True) }}

    {% set revokes_to_execute = get_previous_revokes_which_do_not_exist_in_new_config(new_unique_revokes, previous_unique_revokes) %}
    {% set grants_to_execute = get_new_grants_which_do_not_exist_in_previous_config(new_unique_grants, previous_unique_grants) %}

    {% if (revokes_to_execute | length) > 0 or (grants_to_execute | length) > 0 %}
        {% set execute_revokes_and_grants_query %}

    ---Revokes
    {{revokes_to_execute | join('\n')}}
    ---Grants
    {{grants_to_execute | join('\n')}}

        {% endset %}

        {{ log(execute_revokes_and_grants_query, info=True) }}

        {% do run_query(execute_revokes_and_grants_query) %}

    {% else %} {{ log("No grants or revokes to execute", info=True) }}
    {% endif %}

    {% set drop_temp_config_table_query %}
    DROP TABLE access_management.{{temp_config_table_name}};
    {% endset %}
    {% do run_query(create_config_table_query) %}
    {{ log(drop_temp_config_table_query, info=True) }}
    {% do run_query(drop_temp_config_table_query) %}

{% endmacro %}

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

{% macro get_grants_and_revokes(objects_in_database, config_table_name, should_check_if_config_table_exits=True) %}
    {% set unique_grants = [] %}
    {% set unique_revokes = [] %}

    {% if should_check_if_config_table_exits %}
        {% set query %}
    SELECT * FROM information_schema.tables WHERE table_schema = 'access_management' and table_name = '{{config_table_name}}';
        {% endset %}
        {% set result = run_query(query) %}
        {% if result.rows  | length == 0 %}
            {% set result = {
        'unique_grants': unique_grants,
        'unique_revokes': unique_revokes
    } %}
            {{ return(result) }}
        {% endif %}
    {% endif %}

    {% set query %}
        SELECT json_parse(grants::varchar) as grants, json_parse(revokes::varchar) as revokes
        FROM access_management.{{config_table_name}}
        WHERE database_name || '.' || schema_name || '.' || model_name || '.' ||  CASE
            WHEN lower(materialization) = 'view' THEN 'view'  ELSE 'table' END
        IN ({{ "'" ~ objects_in_database | join("', '") ~ "'" }})
    {% endset %}

    {% set grants_and_revokes = run_query(query) %}
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
    {% else %}
        {{ log("No grants and revokes found in table: " ~ config_table_name, info=True) }}
    {% endif %}
    {% set result = {
        'unique_grants': unique_grants,
        'unique_revokes': unique_revokes
    } %}
    {{ return(result) }}
{% endmacro %}

{% macro get_previous_revokes_which_do_not_exist_in_new_config(new_unique_revokes, previous_unique_revokes) %}
    {% set revokes_to_execute = [] %}
    {% for revoke in previous_unique_revokes %}
        {% if revoke not in new_unique_revokes %}
            {% do revokes_to_execute.append(revoke) %}
        {% endif %}
    {% endfor %}

    {{ return(revokes_to_execute) }}
{% endmacro %}

{% macro get_new_grants_which_do_not_exist_in_previous_config(new_unique_grants, previous_unique_grants) %}
    {% set grants_to_execute = [] %}
    {% for grant in new_unique_grants %}
        {% if grant not in previous_unique_grants %}
            {% do grants_to_execute.append(grant) %}
        {% endif %}
    {% endfor %}

    {{ return(grants_to_execute) }}
{% endmacro %}
