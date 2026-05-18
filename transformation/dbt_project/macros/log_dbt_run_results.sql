{% macro log_dbt_run_results(results) %}
  {% if execute %}
    {# Ambil run_id dari environment variabel, fallback ke invocation_id bawaan dbt #}
    {% set pipeline_run_id = var('pipeline_run_id', invocation_id) %}

    {% for res in results %}
      {% set node = res.node %}
      {# Catat hanya model (stg, int, marts) saja, lewati test, seed, snapshot #}
      {% if node.resource_type == 'model' %}
        {% set started_at = res.timing[0].started_at if res.timing else run_started_at %}
        {% set finished_at = res.timing[-1].completed_at if res.timing else modules.datetime.datetime.utcnow() %}

        {% set rows_processed = res.adapter_response.get('rows_affected', 0) if res.adapter_response else 0 %}
        {% set rows_failed = 1 if res.status == 'error' else 0 %}
        {% set status = 'success' if res.status in ['success', 'pass'] else 'failed' %}
        {% set error_msg = res.message | replace("'", "\\'") if res.message else '' %}

        {% set query %}
          INSERT INTO `{{ target.project }}`.ecommerce_gold.pipeline_audit_log (
            run_id,
            pipeline_stage,
            entity_type,
            source_topic,
            rows_processed,
            rows_failed,
            started_at,
            finished_at,
            status,
            error_message
          ) VALUES (
            '{{ pipeline_run_id }}',
            'dbt',
            '{{ node.name }}',
            NULL,
            {{ rows_processed }},
            {{ rows_failed }},
            TIMESTAMP('{{ started_at }}'),
            TIMESTAMP('{{ finished_at }}'),
            '{{ status }}',
            '{{ error_msg }}'
          );
        {% endset %}

        {% do run_query(query) %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}
