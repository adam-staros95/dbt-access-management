{% macro get_materialization_to_securable_object_type(materialization) %}
    {% set materialization_to_securable_object_type_map = {
        "seed": "table",
        "view": "view",
        "materialized_view": "materialized view",
        "table": "table",
        "streaming_table": "table",
        "incremental": "table",
        "snapshot": "table"
    } %}

    {{ return(materialization_to_securable_object_type_map.get(materialization | trim | lower)) }}
{% endmacro %}

{% macro generate_masking_function_name(config, access_management_database_name, access_management_schema_name) -%}
    {{- access_management_database_name ~ '.' ~
        access_management_schema_name ~ '.' ~
        config['database_name'] ~ '_' ~
        config['schema_name'] ~ '_' ~
        config['alias'] ~ '_' ~
        config['column_name'] -}}
{%- endmacro %}

{% macro get_masking_for_column_type(column_type) %}
    {% set t = column_type | lower %}

    {% if t in ['decimal', 'numeric', 'short', 'byte', 'tinyint', 'smallint', 'int', 'integer', 'bigint', 'long', 'double', 'float', 'real'] %}
        {{ return('NULL') }}

    {% elif t == 'boolean' %} {{ return('FALSE') }}

    {% elif t in ['date', 'timestamp'] %} {{ return('NULL') }}

    {% elif t in ['string', 'varchar', 'char'] %}
        {{ return("CAST('*****' AS STRING)") }}

    {% elif t == 'binary' %} {{ return("CAST(X'2A2A2A2A2A' AS BINARY)") }}

    {% elif t in ['json'] %} {{ return("to_json(named_struct('masked', '*****'))") }}

    {% else %} {{ return('NULL') }}
    {% endif %}
{% endmacro %}
