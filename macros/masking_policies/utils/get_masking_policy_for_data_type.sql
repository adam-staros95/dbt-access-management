{% macro get_masking_policy_for_data_type(column_data_type) %}
    {% set policy_names = dbt_access_management.get_policy_names_constants() %}

    {% set column_data_type = column_data_type | lower | trim %}

    {% set masking_policy = None %}
    {% set unmasking_policy = None %}

    {% if column_data_type in ['varchar', 'character varying', 'nvarchar', 'text'] %}
        {% set masking_policy = policy_names.MASK_VARCHAR %}
        {% set unmasking_policy = policy_names.UNMASK_VARCHAR %}
    {% elif column_data_type in ['char', 'character', 'nchar', 'bpchar'] %}
        {% set masking_policy = policy_names.MASK_CHAR %}
        {% set unmasking_policy = policy_names.UNMASK_CHAR %}
    {% elif column_data_type in ['smallint', 'int2'] %}
        {% set masking_policy = policy_names.MASK_SMALLINT %}
        {% set unmasking_policy = policy_names.UNMASK_SMALLINT %}
    {% elif column_data_type in ['integer', 'int', 'int4'] %}
        {% set masking_policy = policy_names.MASK_INT %}
        {% set unmasking_policy = policy_names.UNMASK_INT %}
    {% elif column_data_type in ['bigint', 'int8'] %}
        {% set masking_policy = policy_names.MASK_BIGINT %}
        {% set unmasking_policy = policy_names.UNMASK_BIGINT %}
    {% elif column_data_type in ['decimal', 'numeric'] %}
        {% set masking_policy = policy_names.MASK_DECIMAL %}
        {% set unmasking_policy = policy_names.UNMASK_DECIMAL %}
    {% elif column_data_type in ['real', 'float4'] %}
        {% set masking_policy = policy_names.MASK_REAL %}
        {% set unmasking_policy = policy_names.UNMASK_REAL %}
    {% elif column_data_type in ['double precision', 'float8', 'float'] %}
        {% set masking_policy = policy_names.MASK_DOUBLE_PRECISION %}
        {% set unmasking_policy = policy_names.UNMASK_DOUBLE_PRECISION %}
    {% elif column_data_type == 'date' %}
        {% set masking_policy = policy_names.MASK_DATE %}
        {% set unmasking_policy = policy_names.UNMASK_DATE %}
    {% elif column_data_type in ['timestamp', 'timestamp without time zone'] %}
        {% set masking_policy = policy_names.MASK_TIMESTAMP %}
        {% set unmasking_policy = policy_names.UNMASK_TIMESTAMP %}
    {% elif column_data_type in ['time without time zone', 'time'] %}
        {% set masking_policy = policy_names.MASK_TIME %}
        {% set unmasking_policy = policy_names.UNMASK_TIME %}
    {% else %}
        {% set masking_policy = None %}
        {% set unmasking_policy = None %}
    {% endif %}

   {% set result = {'masking_policy': masking_policy, 'unmasking_policy': unmasking_policy} %}
   {{ return(result) }}
{% endmacro %}
