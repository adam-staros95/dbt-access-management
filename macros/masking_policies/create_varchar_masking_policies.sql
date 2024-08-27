{% macro create_varchar_masking_policies(currently_existing_project_related_masking_policies, project_unique_id) %}

    {% set masking_policy_name = project_name ~ '_mask_varchar_column_' ~ project_unique_id %}
    {% set unmasking_policy_name = project_name ~ '_unmask_varchar_column_' ~ project_unique_id %}

    {% set create_masking_policy_query %}
    CREATE MASKING POLICY {{masking_policy_name}}
    WITH (_input varchar)
    USING (MD5(_input));
    {% endset %}

    {% set create_unmasking_policy_query %}
    CREATE MASKING POLICY {{unmasking_policy_name}}
    WITH (_input varchar)
    USING (_input);
    {% endset %}

    {% if masking_policy_name not in currently_existing_project_related_masking_policies %}
        {% do run_query(create_masking_policy_query) %}
        {{ log("Masking policy " ~ masking_policy_name ~ " created", info=True) }}
    {% else %}
        {{ log("Masking policy " ~ masking_policy_name ~ " already exists", info=True) }}
    {% endif %}

    {% if unmasking_policy_name not in currently_existing_project_related_masking_policies %}
        {% do run_query(create_unmasking_policy_query) %}
        {{ log("Unmasking policy " ~ unmasking_policy_name ~ " created", info=True) }}
    {% else %}
        {{ log("Masking policy " ~ unmasking_policy_name ~ " already exists", info=True) }}
    {% endif %}

{% endmacro %}
