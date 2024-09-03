{% macro get_policy_names_constants() %}
    {% set policy_names = {
        "MASK_VARCHAR": project_name ~ '_mask_varchar',
        "UNMASK_VARCHAR": project_name ~ '_unmask_varchar',
        "MASK_CHAR": project_name ~ '_mask_char',
        "UNMASK_CHAR": project_name ~ '_unmask_char',
        "MASK_INT": project_name ~ '_mask_int',
        "UNMASK_INT": project_name ~ '_unmask_int',
        "MASK_BIGINT": project_name ~ '_mask_bigint',
        "UNMASK_BIGINT": project_name ~ '_unmask_bigint',
        "MASK_FLOAT": project_name ~ '_mask_float',
        "UNMASK_FLOAT": project_name ~ '_unmask_float',
        "MASK_DECIMAL": project_name ~ '_mask_decimal',
        "UNMASK_DECIMAL": project_name ~ '_unmask_decimal',
        "MASK_DATE": project_name ~ '_mask_date',
        "UNMASK_DATE": project_name ~ '_unmask_date',
        "MASK_TIME": project_name ~ '_mask_time',
        "UNMASK_TIME": project_name ~ '_unmask_time',
        "MASK_TIMESTAMP": project_name ~ '_mask_timestamp',
        "UNMASK_TIMESTAMP": project_name ~ '_unmask_timestamp',
    } %}
    {% do return(policy_names) %}
{% endmacro %}


{% macro get_project_masking_policies_names() %}
    {% set policy_names = get_policy_names_constants() %}
    {% set values = [] %}

    {% for key, value in policy_names.items() %}
        {% set values = values.append(value) %}
    {% endfor %}

    {% do return(values) %}
{% endmacro %}