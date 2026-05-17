{% macro col(col_name, col_type='string') -%}
    {%- if target.type == 'bigquery' -%}
        cast({{ col_name }} as {{ col_type }})
    {%- else -%}
        cast(value:{{ col_name }} as {{ col_type }})
    {%- endif -%}
{%- endmacro %}
