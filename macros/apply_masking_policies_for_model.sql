{% macro apply_masking_policies_for_model() %}
    {% if execute %}
        {% if config.get('materialized') != 'ephemeral' %}

        {% set database_identities = get_database_identities() %}
        {% set users_identities = get_users(database_identities) %}
        {% set roles_identities = get_roles(database_identities) %}

        {% set query_config_table %}
            select c.column_name, c.users_with_access, c.roles_with_access from access_management.pii_dev  as t, t.masking_config as c
        {% endset %}

        {% set query_config_table_result = dbt.run_query(query_config_table) %}

        {% for row in query_config_table_result.rows %}
            {{ log(row['column_name'] ~ ' ' ~ row['users_with_access'] ~ ' ' ~ row ['roles_with_access'], info=True) }}
            {{ log('----', info=True) }}
        {% endfor %}

        {% else %}
            {{ log("Skipping assigning permissions for ephemeral model", info=False) }}
        {% endif %}
    {% endif %}
{% endmacro %}

{% macro get_users(database_identities) %}
    {% set users = [] %}
    {% for identity in database_identities %}
        {% for key, value in identity.items() %}
            {% if value == 'user' %}
                {% do users.append(identity) %}
            {% endif %}
        {% endfor %}
    {% endfor %}
    {{ return(users) }}
{% endmacro %}

{% macro get_roles(database_identities) %}
    {% set roles = [] %}
    {% for identity in database_identities %}
        {% for key, value in identity.items() %}
            {% if value == 'role' %}
                {% do roles.append(identity) %}
            {% endif %}
        {% endfor %}
    {% endfor %}
    {{ return(roles) }}
{% endmacro %}