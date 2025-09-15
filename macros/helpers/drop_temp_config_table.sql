{% macro drop_temp_config_table(database_name, schema_name, temp_config_table_name) %}
    {% set drop_temp_config_access_management_table_query %}
        DROP TABLE {{database_name}}.{{schema_name}}.{{temp_config_table_name}};
    {% endset %}
    {{ log(drop_temp_config_access_management_table_query, info=True) }}
    {% do run_query(drop_temp_config_access_management_table_query) %}
{% endmacro %}
