{#/* Based on the incremental materialization from dbt. */#}
{% macro pageup__timestamp_incremental_delete(target_relation, tmp_relation) -%}

  {%- set unique_key = config.require('unique_key') -%}

  delete
  from {{ target_relation }}
  where ({{ unique_key }}) in (
    select ({{ unique_key }})
    from {{ tmp_relation.include(schema=False) }}
  );

{%- endmacro %}

{% materialization timestamp_incremental, default -%}
  {%- set unique_key = config.require('unique_key') -%}
  {%- set timestamp_suffix = config.get('timestamp_suffix') -%}
  {%- if timestamp_suffix is none -%}
    {%- set timestamp_suffix = '_data_pipeline_timestamp' -%}
  {%- endif -%}

  {%- set identifier = model['alias'] -%}
  {%- set tmp_identifier = identifier + '__dbt_timestamp_incremental_tmp' -%}

  {%- set old_relation = adapter.get_relation(database=database,schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier, schema=schema, type='table') -%}
  {%- set tmp_relation = api.Relation.create(identifier=tmp_identifier,
                                                 schema=schema, type='table') -%}

  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}
  {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}

  {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
  {%- set exists_not_as_table = (old_relation is not none and not old_relation.is_table) -%}

  {%- set should_truncate = (non_destructive_mode and full_refresh_mode and exists_as_table) -%}
  {%- set should_drop = (not should_truncate and (full_refresh_mode or exists_not_as_table)) -%}
  {%- set force_create = (flags.FULL_REFRESH and not flags.NON_DESTRUCTIVE) -%}

  -- setup
  {% if old_relation is none -%}
    -- noop
  {%- elif should_truncate -%}
    {{ adapter.truncate_relation(old_relation) }}
  {%- elif should_drop -%}
    {{ adapter.drop_relation(old_relation) }}
    {%- set old_relation = none -%}
  {%- endif %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% if force_create or old_relation is none -%}
    {%- call statement('main') -%}
      {{ create_table_as(False, target_relation, sql) }}
    {%- endcall -%}
  {%- else -%}  
     {% set dest_columns = adapter.get_columns_in_table(schema, identifier) %}
     {%- call statement() -%}
       {% set tmp_table_sql -%}
         {#/* We are using a subselect instead of a CTE here to allow PostgreSQL to use indexes. */-#}
         select * 
         from (
             {{ sql }}
             ) as dbt_incr_sbq

         {#/* Generate a check for each timestamp column to ensure we only update changed rows */-#}
         {#/* Note: it doesnt have to be a timestamp, any comparable type will work too, as long as newer rows have a bigger value */-#}
         {%- for col in dest_columns if col.name.endswith(timestamp_suffix) %}
           {% if loop.first %}where {% else %}   or {% endif -%}
             {{ col.quoted }} > (select max({{ col.quoted }}) from {{ target_relation }})
         {%- endfor %}

       {%- endset %}

       {{ dbt.create_table_as(True, tmp_relation, tmp_table_sql) }}

     {%- endcall -%}

     {{ adapter.expand_target_column_types(temp_table=tmp_identifier,
                                           to_schema=schema,
                                           to_table=identifier) }}

     {%- call statement('main') -%}
       {% set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') %}

       {% if unique_key is not none -%}

         {{ dbt__incremental_delete(target_relation, tmp_relation) }}

       {%- endif %}

       insert into {{ target_relation }} ({{ dest_cols_csv }})
       (
         select {{ dest_cols_csv }}
         from {{ tmp_relation.include(schema=False) }}
       );
     {% endcall %}
  {%- endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

{%- endmaterialization %}