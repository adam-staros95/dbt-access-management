{% macro create_project_related_masking_policies() %}

    {% set project_masking_policies_names = get_project_masking_policies_names() %}

    {% set get_existing_project_related_masking_policies_query %}
        select policy_name from SVV_MASKING_POLICY
        where policy_database = current_database()
            and policy_name in ({{ "'" ~ project_masking_policies_names | join("', '") ~ "'" }});
    {% endset %}

    {% set get_existing_project_related_masking_policies_query_result = run_query(get_existing_project_related_masking_policies_query) %}

    {% set currently_existing_project_related_masking_policies = [] %}
    {% for row in get_existing_project_related_masking_policies_query_result.rows %}
        {% do currently_existing_project_related_masking_policies.append(row.policy_name) %}
    {% endfor %}

    {% do create_bigint_masking_policies(currently_existing_project_related_masking_policies) %}
    {% do create_char_masking_policies(currently_existing_project_related_masking_policies) %}
    {% do create_date_masking_policies(currently_existing_project_related_masking_policies) %}
    {% do create_decimal_masking_policies(currently_existing_project_related_masking_policies) %}
    {% do create_double_precision_masking_policies(currently_existing_project_related_masking_policies) %}
    {% do create_int_masking_policies(currently_existing_project_related_masking_policies) %}
    {% do create_real_masking_policies(currently_existing_project_related_masking_policies) %}
    {% do create_smallint_masking_policies(currently_existing_project_related_masking_policies) %}
    {% do create_time_masking_policies(currently_existing_project_related_masking_policies) %}
    {% do create_timestamp_masking_policies(currently_existing_project_related_masking_policies) %}
    {% do create_varchar_masking_policies(currently_existing_project_related_masking_policies) %}

{% endmacro %}
