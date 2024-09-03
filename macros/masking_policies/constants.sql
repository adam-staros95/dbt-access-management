{% macro get_policy_names_constants() %}
    {% set policy_names = {
        "MASK_VARCHAR": project_name ~ '_mask_varchar',
        "UNMASK_VARCHAR": project_name ~ '_unmask_varchar',
        "MASK_CHAR": project_name ~ '_mask_char',
        "UNMASK_CHAR": project_name ~ '_unmask_char',
        "MASK_INT": project_name ~ '_mask_int',
        "UNMASK_INT": project_name ~ '_unmask_int',
        "MASK_REAL": project_name ~ '_mask_real',
        "UNMASK_REAL": project_name ~ '_unmask_real',
        "MASK_SMALLINT": project_name ~ '_mask_smallint',
        "UNMASK_SMALLINT": project_name ~ '_unmask_smallint',
        "MASK_BIGINT": project_name ~ '_mask_bigint',
        "UNMASK_BIGINT": project_name ~ '_unmask_bigint',
        "MASK_DOUBLE_PRECISION": project_name ~ '_mask_double_precision',
        "UNMASK_DOUBLE_PRECISION": project_name ~ '_unmask_double_precision',
        "MASK_DATE": project_name ~ '_mask_date',
        "UNMASK_DATE": project_name ~ '_unmask_date',
        "MASK_DECIMAL": project_name ~ '_mask_decimal',
        "UNMASK_DECIMAL": project_name ~ '_unmask_decimal',
        "MASK_TIME": project_name ~ '_mask_time',
        "UNMASK_TIME": project_name ~ '_unmask_time',
        "MASK_TIMETZ": project_name ~ '_mask_timetz',
        "UNMASK_TIMETZ": project_name ~ '_unmask_timetz',
        "MASK_TIMESTAMP": project_name ~ '_mask_timestamp',
        "UNMASK_TIMESTAMP": project_name ~ '_unmask_timestamp',
        "MASK_TIMESTAMPTZ": project_name ~ '_mask_timestamptz',
        "UNMASK_TIMESTAMPTZ": project_name ~ '_unmask_timestamptz',
        "MASK_BOOLEAN": project_name ~ '_mask_boolean',
        "UNMASK_BOOLEAN": project_name ~ '_unmask_boolean',
        "MASK_SUPER": project_name ~ '_mask_super',
        "UNMASK_SUPER": project_name ~ '_unmask_super',
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