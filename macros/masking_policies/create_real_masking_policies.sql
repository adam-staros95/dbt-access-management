{% macro create_real_masking_policies(currently_existing_project_related_masking_policies) %}

    {% set policy_names = dbt_access_management.get_policy_names_constants() %}
    {% set masking_policy_name = policy_names.MASK_REAL %}
    {% set unmasking_policy_name =policy_names.UNMASK_REAL %}

    {% set create_masking_policy_query %}
    CREATE MASKING POLICY {{masking_policy_name}}
    WITH (_input real)
    USING (-999::real);
    {% endset %}

    {% set create_unmasking_policy_query %}
    CREATE MASKING POLICY {{unmasking_policy_name}}
    WITH (_input real)
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
