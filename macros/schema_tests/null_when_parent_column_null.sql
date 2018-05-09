{#/*
Test that a dependant column in a table is null whenever the "parent" column is null.
Parent column is typically an Id/foreign key of a table that has been denormalised.

Arguments:
    model: table model (not needed when called from schema.yml)
    parent: The parent column, usually an Id column
    dependant: The column that is dependant on the parent column
    bi_directional: If true, the parent column and dependant column should always be either both null or both not null. Default false
*/#}
{% macro test_null_when_parent_column_null(model, parent, dependant, bi_directional = False) %}
    
    SELECT count(1)
    FROM (SELECT 1 AS error
          FROM   {{model.schema}}.{{model.name}}
          WHERE  {{model.name}}.{{parent}}    IS NULL
            AND  {{model.name}}.{{dependant}} IS NOT NULL
          {% if bi_directional -%}
          UNION ALL
          SELECT 1 AS error
          FROM   {{model.schema}}.{{model.name}}
          WHERE  {{model.name}}.{{parent}}    IS NOT NULL
            AND  {{model.name}}.{{dependant}} IS NULL
          {%- endif %}) AS errors
          
{% endmacro %}
