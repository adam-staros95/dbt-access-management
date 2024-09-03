{% macro create_masking_policies(currently_existing_project_related_masking_policies, masking_policy_name, create_masking_policy_query, unmasking_policy_name, create_unmasking_policy_query) %}

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
