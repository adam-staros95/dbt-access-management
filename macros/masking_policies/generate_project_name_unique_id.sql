{% macro generate_project_name_unique_id(project_name) %}
    {% set full_hash = local_md5(project_name) %}
    {% set unique_id = full_hash[:5] %}
    {{ return(unique_id) }}
{% endmacro %}