{% macro validate_configured_identities(database_name, schema_name, config_table_name, should_stop_execution=True) %}
    {{ adapter.dispatch('validate_configured_identities')(database_name, schema_name, config_table_name, should_stop_execution) }}
{% endmacro %}

{% macro redshift__validate_configured_identities(database_name, schema_name, config_table_name, should_stop_execution) %}
    {% if execute %}
        {% set database_identities = get_database_identities() %}
        {% set config_table_identities = get_config_table_identities(database_name, schema_name, config_table_name) %}
        {% set database_identities_mapped_to_list_of_strings = map_list_of_dicts_to_list_of_tuples(database_identities) %}
        {% set config_table_identities_mapped_to_list_of_strings = map_list_of_dicts_to_list_of_tuples(config_table_identities) %}

        {% set issues = [] %}
        {% for config_table_identity in config_table_identities_mapped_to_list_of_strings | unique %}
            {% if config_table_identity not in database_identities_mapped_to_list_of_strings %}
                {% do issues.append(config_table_identity) %}
            {% endif %}
        {% endfor %}

        {% if issues | length > 0 %}
            {% set issue_message = "The following identities configured in DBT access management, but do not exist in database:" ~ issues | join(', ') %}
            {% if should_stop_execution %}
                {{ exceptions.raise_compiler_error(issue_message) }}
            {% else %} {{ log(issue_message, info=True) }}
            {% endif %}
        {% else %}
            {{ log("No issues found, all identities configured in DBT access management, exist in database", info=True) }}
        {% endif %}
    {% endif %}
{% endmacro %}

{% macro map_list_of_dicts_to_list_of_tuples(identities) %}
    {% set identity_strings = [] %}
    {% for identity in identities %}
        {% do identity_strings.append(identity['identity_type'] ~ ":" ~ identity['identity_name']) %}
    {% endfor %}
    {{ return(identity_strings) }}
{% endmacro %}

{% macro databricks__validate_configured_identities(database_name, schema_name, config_table_name, should_stop_execution) %}
    {% if execute %}
        {% set ts = run_started_at.strftime("%Y%m%d%H%M%S") %}
        {% set test_identity_check_table_name = 'test_identity_check_' ~ ts %}
        {% do log("Checking if identity exists using table: " ~ database_name ~ "." ~ schema_name ~ "." ~ test_identity_check_table_name, info=True) %}
        {% set create_test_identity_check_table_query %}
            begin
            create or replace table {{database_name}}.{{schema_name}}.{{test_identity_check_table_name}} (id int);
            insert into {{database_name}}.{{schema_name}}.{{test_identity_check_table_name}} values (1);
            end;
        {% endset %}

        {% do run_query(create_test_identity_check_table_query) %}
        {% set config_table_identities = get_config_table_identities(database_name, schema_name, config_table_name) %}
        {% set unique_identities = config_table_identities | map(attribute='identity_name') | unique | list %}

        {% set missing = [] %}
        {% for identity in unique_identities %}
            {% set grant_stmt %}
                GRANT SELECT ON TABLE {{ database_name }}.{{ schema_name }}.{{ test_identity_check_table_name }}
                TO `{{ identity }}`;
            {% endset %}

            {% do log("Checking if identity exists: " ~ identity, info=True) %}

            {% if execute %} {% set result = run_query(grant_stmt) %} {% endif %}
        {% endfor %}

        {% set drop_stmt %}
            drop table if exists {{ database_name }}.{{ schema_name }}.{{ test_identity_check_table_name }};
        {% endset %}

        {% do run_query(drop_stmt) %}
    {% endif %}
{% endmacro %}

{% macro get_config_table_identities(database_name, schema_name, config_table_name) %}
    {% set query_config_table_identities %}
        SELECT identity_name AS identity_name,
               identity_type AS identity_type
        FROM {{ database_name }}.{{ schema_name }}.{{ config_table_name }}
    {% endset %}

    {% set query_result = dbt.run_query(query_config_table_identities) %}

    {% set config_table_identities = [] %}
    {% for row in query_result.rows %}
        {% do config_table_identities.append(
            {
                'identity_name': row.identity_name,
                'identity_type': row.identity_type
            }
        ) %}
    {% endfor %}

    {{ return(config_table_identities) }}
{% endmacro %}
