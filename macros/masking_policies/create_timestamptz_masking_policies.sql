{% macro create_timestamptz_masking_policies(currently_existing_project_related_masking_policies) %}

    {% set policy_names = dbt_access_management.get_policy_names_constants() %}
    {% set masking_policy_name = policy_names.MASK_TIMESTAMPTZ %}
    {% set unmasking_policy_name = policy_names.UNMASK_TIMESTAMPTZ %}

    {% set create_masking_policy_query %}
    CREATE MASKING POLICY {{masking_policy_name}}
    WITH (_input timestamptz)
    USING ('0001-01-01 00:00:00'::timestamptz);
    {% endset %}

    {% set create_unmasking_policy_query %}
    CREATE MASKING POLICY {{unmasking_policy_name}}
    WITH (_input timestamptz)
    USING (_input);
    {% endset %}

    {% do create_masking_policies(
        currently_existing_project_related_masking_policies = currently_existing_project_related_masking_policies,
        masking_policy_name = masking_policy_name,
        create_masking_policy_query = create_masking_policy_query,
        unmasking_policy_name = unmasking_policy_name,
        create_unmasking_policy_query = create_unmasking_policy_query
    ) %}

{% endmacro %}
