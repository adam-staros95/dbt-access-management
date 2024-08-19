{% macro validate_configured_entities(config_table_name, should_stop_execution=True) %}
    {% if execute %}
        {% set query_system_tables_entities %}
            SELECT usename AS entity_name, 'user' AS entity_type FROM PG_USER
            UNION ALL
            SELECT role_name AS entity_name, 'role' AS entity_type FROM SVV_ROLES
            UNION ALL
            SELECT groname AS entity_name, 'group' AS entity_type FROM PG_GROUP;
        {% endset %}

        {% set query_system_tables_entities_result = dbt.run_query(query_system_tables_entities) %}

        {% set system_tables_entities = [] %}

        {% for row in query_system_tables_entities_result.rows %}
            {% do system_tables_entities.append((row['entity_type'], row['entity_name'])) %}
        {% endfor %}

        {% set query_sconfig_table_entities %}
            SELECT entity_name AS entity_name, entity_type AS entity_type
            FROM access_management.{{config_table_name}};
        {% endset %}

        {% set query_sconfig_table_entities_result = dbt.run_query(query_sconfig_table_entities) %}

        {% set config_table_entities = [] %}

        {% for row in query_sconfig_table_entities_result.rows %}
            {% do config_table_entities.append((row['entity_type'], row['entity_name'])) %}
        {% endfor %}

        {% set issues = [] %}

        {% for element in config_table_entities | unique %}
            {% if element not in system_tables_entities %}
                {% do issues.append(element) %}
            {% endif %}
        {% endfor %}

        {% if issues | length > 0 %}

            {% set issue_message = "The following entities configured in DBT access management, but do not exist in database:" ~ issues | join(', ') %}

            {% if should_stop_execution %}
                {{ exceptions.raise_compiler_error(issue_message) }}
            {% else %} {{ log(issue_message, info=True) }}
            {% endif %}
        {% else %}
            {{ log("No issues found, all entities configured in DBT access management, exist in database", info=True) }}
        {% endif %}
    {% endif %}
{% endmacro %}
