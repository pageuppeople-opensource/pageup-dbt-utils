{#/*
Test that a column has a matching timestamp column that behaves like a timestamp.
Timestamp column name should be the column name > remove '_id' if exists > add '_data_pipeline_timestamp'.
Columns and their matching timestamp columns should either both be null or neither be null.

Example:
    Column 'person_id' should have a matching timestamp column called 'person_data_pipeline_timestamp'
    Column 'organisation' should have a matching timestamp column called 'organisation_data_pipeline_timestamp'

Arguments:
    model: table model (not needed when called from schema.yml)
    arg: The column that should have a matching timestamp
*/#}
{% macro test_has_timestamp(model, arg) %}
    
    {%- call statement('get_timestamp_column_name', fetch_result=True) -%}

        SELECT column_name AS column_name
        FROM   information_schema.columns
        WHERE  table_schema = '{{model.schema}}'
          AND  table_name   = '{{model.name}}'
          AND  column_name  = regexp_replace('{{arg}}', '_id$', '') || '_data_pipeline_timestamp'

    {%- endcall -%}

    {%- set column_name_query = load_result('get_timestamp_column_name') -%}
    {%- if column_name_query -%}
        {%- if column_name_query['data'] and column_name_query['data'][0] -%}
            {{- pageup_dbt_utils.test_null_when_parent_column_null(model, 
                                                                   arg, 
                                                                   column_name_query['data'][0][0], 
                                                                   bi_directional=true) -}} 
        {%- else -%}
            {{- log('FAIL: Timestamp column not found for ' ~ model.schema ~ '.' ~ model.table ~ '.' ~ arg, info=True) -}}
            SELECT 1 --fail
        {% endif -%}
    {% endif -%}

{% endmacro %}