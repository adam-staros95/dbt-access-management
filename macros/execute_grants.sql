{% macro execute_grants() %}
    {% do adapter.dispatch('execute_grants')() %}
{% endmacro %}
