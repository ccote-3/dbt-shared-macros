{% macro log_dbt_run_results(audit_table='SystemLog.AuditLog') %}
  {% if execute and results is defined %}
    {%- set logged_models = [] -%}
    
    {% for res in results if res.node.resource_type == 'model' and res.status == 'success' %}
      {%- set target_schema = res.node.config.schema if res.node.config.schema is not none else res.node.schema -%}
      {%- set target_name = res.node.config.alias if res.node.config.alias is not none else res.node.name -%}
      {%- set full_table = target_schema ~ '.' ~ target_name -%}

      {# Skip logging for audit log tables and models already processed in this run #}
      {%- if target_name not in ['AuditLog', 'DBTAuditLog'] and full_table not in logged_models -%}
        {%- do logged_models.append(full_table) -%}
        
        {# 1. Pull the 2 most recent history events #}
        {%- set history_sql -%}
          DESCRIBE HISTORY {{ full_table }} LIMIT 2
        {%- endset -%}
        
        {%- set history_res = run_query(history_sql) -%}

        {%- if history_res and history_res.rows | length > 0 -%}
          {# Find newest non-OPTIMIZE transaction #}
          {%- set ns = namespace(target_row=none) -%}
          {%- for h_row in history_res.rows -%}
            {%- set op = (h_row['operation'] | string) | upper -%}
            {%- if 'OPTIMIZE' not in op and ns.target_row is none -%}
              {%- set ns.target_row = h_row -%}
            {%- endif -%}
          {%- endfor -%}

          {%- if ns.target_row is none -%}
            {%- set ns.target_row = history_res.rows[0] -%}
          {%- endif -%}

          {%- set target_row = ns.target_row -%}
          
          {# Normalize both timestamps to clean numeric format: YYYYMMDDHHMMSS #}
          {%- set commit_clean = (target_row['timestamp'] | string) | replace('-', '') | replace(':', '') | replace('T', '') | replace('Z', '') | replace(' ', '') | truncate(14, True, '') -%}
          {%- set run_clean = (run_started_at | string) | replace('-', '') | replace(':', '') | replace('T', '') | replace('Z', '') | replace(' ', '') | truncate(14, True, '') -%}
          
          {%- set duration_seconds = (res.execution_time | round(2)) if res.execution_time is not none else 0.0 -%}

          {%- set num_inserted = 0 -%}
          {%- set num_updated = 0 -%}
          {%- set num_deleted = 0 -%}
          {%- set num_source = 0 -%}
          {%- set op_name = target_row["operation"] -%}

          {# Check if the Delta commit belongs to this dbt run #}
          {%- if (commit_clean | int) >= (run_clean | int) -%}
            {%- set raw_metrics = '' ~ target_row['operationMetrics'] -%}

            {%- if 'numTargetRowsInserted' in raw_metrics -%}
              {%- set val = raw_metrics.split('numTargetRowsInserted')[1].split(',')[0].replace('"', '').replace(':', '').replace('->', '').replace('}', '').strip() -%}
              {%- set num_inserted = val | int -%}
            {%- elif 'numOutputRows' in raw_metrics -%}
              {%- set val = raw_metrics.split('numOutputRows')[1].split(',')[0].replace('"', '').replace(':', '').replace('->', '').replace('}', '').strip() -%}
              {%- set num_inserted = val | int -%}
            {%- endif -%}

            {%- if 'numTargetRowsUpdated' in raw_metrics -%}
              {%- set val = raw_metrics.split('numTargetRowsUpdated')[1].split(',')[0].replace('"', '').replace(':', '').replace('->', '').replace('}', '').strip() -%}
              {%- set num_updated = val | int -%}
            {%- endif -%}

            {%- if 'numTargetRowsDeleted' in raw_metrics -%}
              {%- set val = raw_metrics.split('numTargetRowsDeleted')[1].split(',')[0].replace('"', '').replace(':', '').replace('->', '').replace('}', '').strip() -%}
              {%- set num_deleted = val | int -%}
            {%- endif -%}

            {%- if 'numSourceRows' in raw_metrics -%}
              {%- set val = raw_metrics.split('numSourceRows')[1].split(',')[0].replace('"', '').replace(':', '').replace('->', '').replace('}', '').strip() -%}
              {%- set num_source = val | int -%}
            {%- elif 'numOutputRows' in raw_metrics -%}
              {%- set val = raw_metrics.split('numOutputRows')[1].split(',')[0].replace('"', '').replace(':', '').replace('->', '').replace('}', '').strip() -%}
              {%- set num_source = val | int -%}
            {%- endif -%}
          {%- else -%}
            {# Zero-change run: Delta wrote no new commit -> record 0 for all counters #}
            {%- set op_name = 'MERGE (NO-OP)' -%}
          {%- endif -%}

          {# 2. Insert run record into AuditLog #}
          {%- set insert_sql -%}
            INSERT INTO {{ audit_table }} VALUES (
              '{{ invocation_id }}',
              '{{ full_table }}',
              current_timestamp(),
              '{{ op_name }}',
              {{ num_inserted }},
              {{ num_updated }},
              {{ num_deleted }},
              {{ num_source }},
              {{ duration_seconds }}
            )
          {%- endset -%}
          
          {%- do run_query(insert_sql) -%}
        {%- endif -%}
      {%- endif -%}
    {% endfor %}
  {% endif %}
{% endmacro %}