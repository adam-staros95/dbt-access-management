{% macro create_project_related_masking_policies() %}
    {% set project_unique_id = generate_project_name_unique_id(project_name) %}
    {{ log("Unique project id " ~ project_unique_id ~ " for project name " ~ project_name, info=True) }}

    {% set get_existing_project_related_masking_policies_query %}
    select policy_name from SVV_MASKING_POLICY where policy_database = current_database() and policy_name ilike '{{project_name}}_%_{{project_unique_id}}'
    {% endset %}
    {% set get_existing_project_related_masking_policies_query_result = run_query(get_existing_project_related_masking_policies_query) %}

    {% set currently_existing_project_related_masking_policies = [] %}
    {% for row in get_existing_project_related_masking_policies_query_result.rows %}
        {% do currently_existing_project_related_masking_policies.append(row.policy_name) %}
    {% endfor %}

    {% do create_varchar_masking_policies(currently_existing_project_related_masking_policies, project_unique_id) %}

{% endmacro %}