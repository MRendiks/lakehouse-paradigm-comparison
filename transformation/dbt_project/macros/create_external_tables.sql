{% macro create_external_tables() %}
    {% if target.type == 'snowflake' %}
        {% set entities = [
            'order', 'order_item', 'payment', 'review',
            'customer', 'product', 'product_category', 'seller', 'geolocation'
        ] %}
        
        {% for entity in entities %}
            {% set sql %}
                CREATE OR REPLACE EXTERNAL TABLE LAKEHOUSE_RAW.SILVER."{{ entity | upper }}"
                LOCATION = @LAKEHOUSE_RAW.SILVER.GCS_SILVER_STAGE/{{ entity }}/
                REFRESH_ON_CREATE = false
                AUTO_REFRESH = false
                FILE_FORMAT = (TYPE = PARQUET)
                TABLE_FORMAT = DELTA;
            {% endset %}
            
            {% do run_query(sql) %}
            {{ log("Created Snowflake External Table: " ~ entity, info=True) }}
        {% endfor %}
    {% else %}
        {{ log("BigQuery external tables are managed via Terraform", info=True) }}
    {% endif %}
{% endmacro %}
