{% macro get_columns_info_for_masking_configs(masking_configs) %}
    {% if not masking_configs %} {{ return([]) }} {% endif %}

    {% set databases = [] %}
    {% for config in masking_configs %}
        {% if config.database_name not in databases %}
            {% do databases.append(config.database_name) %}
        {% endif %}
    {% endfor %}

    {% set conditions = [] %}
    {% for config in masking_configs %}
        {% set condition = config.database_name ~ '.' ~ config.schema_name ~ '.' ~ config.alias ~ '.' ~ config.column_name %}
        {% if condition not in conditions %}
            {% do conditions.append(condition) %}
        {% endif %}
    {% endfor %}

    {% set queries = [] %}
    {% for database in databases %}
        {% set q %}
            select
                table_catalog,
                table_schema,
                table_name,
                column_name,
                lower(data_type) as data_type
            from {{ database }}.information_schema.columns
            where table_catalog || '.' || table_schema || '.' || table_name || '.' || column_name in ({{ "'" ~ conditions | join("', '") ~ "'" }})
        {% endset %}
        {% do queries.append(q) %}
    {% endfor %}
    {% set union_query = queries | join(' union all ') %}
    {% set query_result = dbt.run_query(union_query) %}

    {% set columns_info = {} %}
    {% for row in query_result.rows %}
        {% set key = row.table_catalog ~ '.' ~ row.table_schema ~ '.' ~ row.table_name ~ '.' ~ row.column_name %}
        {% set _ = columns_info.update({key: row.data_type}) %}
    {% endfor %}

    {{ return(columns_info) }}

{% endmacro %}
