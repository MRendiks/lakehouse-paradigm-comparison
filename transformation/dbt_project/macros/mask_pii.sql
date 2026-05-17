{% macro mask_pii(column_name, data_type='string') %}
    {% if target.name == 'prod' %}
        {{ column_name }}
    {% else %}
        {% if data_type == 'string' %}
            CONCAT(SUBSTR(CAST({{ column_name }} AS STRING), 1, 4), '****-PII-MASKED-****')
        {% else %}
            NULL
        {% endif %}
    {% endif %}
{% endmacro %}
