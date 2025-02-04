{% macro check_snapshot_table_has_masking_policies_attached(schema_name, table_name) %}

    {% if config.get('materialized') == 'snapshot' %}
        {% set has_policies_attached_query -%}
                    SELECT EXISTS (
                    SELECT table_name
                    FROM SVV_ATTACHED_MASKING_POLICY
                    WHERE schema_name = '{{ schema_name }}'
                    AND table_name = '{{ table_name }}'
                );
        {%- endset %}

        {% set has_policies_attached_query_results = dbt.run_query(has_policies_attached_query) %}
        {{ return(has_policies_attached_query_results.rows[0][0]) }}
    {% endif %}

    {{ return(false) }}
{% endmacro %}
