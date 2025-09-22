{% macro get_previous_unique_revokes_which_do_not_exist_in_new_config(new_unique_revokes, previous_unique_revokes) %}
    {% set revokes_to_execute = [] %}
    {% for revoke in previous_unique_revokes %}
        {% if revoke not in new_unique_revokes %}
            {% do revokes_to_execute.append(revoke) %}
        {% endif %}
    {% endfor %}

    {{ return(revokes_to_execute) }}
{% endmacro %}

{% macro get_new_unique_grants_which_do_not_exist_in_previous_config(new_unique_grants, previous_unique_grants) %}
    {% set grants_to_execute = [] %}
    {% for grant in new_unique_grants %}
        {% if grant not in previous_unique_grants %}
            {% do grants_to_execute.append(grant) %}
        {% endif %}
    {% endfor %}

    {{ return(grants_to_execute) }}
{% endmacro %}
