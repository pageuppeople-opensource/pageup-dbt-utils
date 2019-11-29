{% macro build_timestamp_incremental_sql(target_relation, sql) -%}
    {%- set timestamp_suffix = var('TIMESTAMP_SUFFIX', 'model_timestamp') -%}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {#- We are using a subselect instead of a CTE here to allow PostgreSQL to use indexes. #}
    select * 
    from (
        {{ sql }}
    ) as dbt_incr_sbq

    {#- Generate a check for each timestamp column to ensure we only update changed rows -#}
    {#- Note: it doesnt have to be a timestamp, any comparable type will work too, as long as newer rows have a bigger value -#}
    {%- for col in dest_columns if col.name.endswith(timestamp_suffix) %}
    {% if loop.first %}where {% else %}   or {% endif -%}
        {{ col.quoted }} > (select max({{ col.quoted }}) from {{ target_relation }})
    {%- endfor %}
{%- endmacro %}

{% materialization timestamp_incremental, default -%}

  {% set unique_key = config.get('unique_key') %}
  {% set full_refresh_mode = flags.FULL_REFRESH %}

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}
  {% if existing_relation is none %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view or full_refresh_mode %}
      {#-- Make sure the backup doesn't exist so we don't encounter issues with the rename below #}
      {% set backup_identifier = existing_relation.identifier ~ "__dbt_backup" %}
      {% set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) %}
      {% do adapter.drop_relation(backup_relation) %}

      {% do adapter.rename_relation(target_relation, backup_relation) %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
      {% do to_drop.append(backup_relation) %}
  {% else %}
      {% set tmp_relation = make_temp_relation(target_relation) %}
      {# BEGIN MODIFIED CODE #}
      {% set incremental_sql = pageup_dbt_utils.build_timestamp_incremental_sql(target_relation, sql) %}
      {% do run_query(create_table_as(True, tmp_relation, incremental_sql)) %}
      {# END MODIFIED CODE #}
      {% do adapter.expand_target_column_types(
             from_relation=tmp_relation,
             to_relation=target_relation) %}
      {% set build_sql = incremental_upsert(tmp_relation, target_relation, unique_key=unique_key) %}
  {% endif %}

  {% call statement("main") %}
      {{ build_sql }}
  {% endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}