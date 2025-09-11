{% macro mask_data() %} {% do adapter.dispatch('mask_data')() %} {% endmacro %}
