{% macro col(col_name, col_type='string') -%}
    -- Detects the active warehouse at runtime and emits the correct column accessor.
    -- BigQuery exposes Delta Lake columns as native fields (safe_cast).
    -- Snowflake wraps them inside a single VARIANT column (cast via value:).
    -- One macro, two engines — single dbt codebase.
    {%- if target.type == 'bigquery' -%}
        safe_cast({{ col_name }} as {{ col_type }})
    {%- else -%}
        cast(value:{{ col_name }} as {{ col_type }})
    {%- endif -%}
{%- endmacro %}
