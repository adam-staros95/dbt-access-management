{% macro databricks__mask_data(access_management_database_name, access_management_schema_name) %}
    {% if execute %}
        {% if config.get('materialized') != 'ephemeral' %}
            {{ log("SKIPPING databricks__mask_data", info=True) }}
        {% endif %}
    {% endif %}
{% endmacro %}
