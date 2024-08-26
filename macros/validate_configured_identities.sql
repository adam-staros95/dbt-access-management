{% macro validate_configured_identities(config_table_name, should_stop_execution=True) %}
    {% if execute %}
        {% set database_identities = get_database_identities() %}

        {% set query_config_table_identities %}
            SELECT identity_name AS identity_name, identity_type AS identity_type
            FROM access_management.{{config_table_name}};
        {% endset %}

        {% set query_config_table_identities_result = dbt.run_query(query_config_table_identities) %}
        {% set config_table_identities = [] %}

        {% for row in query_config_table_identities_result.rows %}
            {% do config_table_identities.append({'identity_name': row.identity_name, 'identity_type': row.identity_type}) %}
        {% endfor %}

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
            {{ log("No issues found, all identities configured in dbt-access-management, exist in database", info=True) }}
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
