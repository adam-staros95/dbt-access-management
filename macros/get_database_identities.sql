{% macro get_database_identities() %}

    {% set query_identities %}
        SELECT usename AS identity_name, 'user' AS identity_type FROM PG_USER
        UNION ALL
        SELECT role_name AS identity_name, 'role' AS identity_type FROM SVV_ROLES
        UNION ALL
        SELECT groname AS identity_name, 'group' AS identity_type FROM PG_GROUP;
    {% endset %}

    {% set identities_query_result = dbt.run_query(query_identities) %}

    {% set identities = [] %}

    {% for row in identities_query_result.rows %}
        {% do identities.append({'identity_name': row.identity_name, 'identity_type': row.identity_type}) %}
    {% endfor %}

    {{ return(identities) }}
{% endmacro %}

{% macro get_users(database_identities) %}
    {% set users = [] %}
    {% for identity in database_identities %}
        {% if identity['identity_type'] == 'user' %}
            {% do users.append(identity['identity_name']) %}
        {% endif %}
    {% endfor %}
    {{ return(users) }}
{% endmacro %}

{% macro get_roles(database_identities) %}
    {% set roles = [] %}
    {% for identity in database_identities %}
        {% if identity['identity_type'] == 'role' %}
            {% do roles.append(identity['identity_name']) %}
        {% endif %}
    {% endfor %}
    {{ return(roles) }}
{% endmacro %}