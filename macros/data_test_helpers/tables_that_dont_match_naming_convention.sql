{#/*
Returns the names of all tables in target schema that do not match required naming convention
*/#}
{% macro tables_that_dont_match_naming_convention() %}

WITH table_names AS (
    SELECT table_schema,
           table_name
    FROM   information_schema.tables
    WHERE  table_schema LIKE '{{target.schema}}%'
)
SELECT table_schema || '.' || table_name
FROM   table_names
EXCEPT
SELECT table_schema || '.' || table_name
FROM   table_names
WHERE LOWER(table_name)                                  = table_name  -- only lowercase
  AND REGEXP_REPLACE(table_name, '[^a-z0-9_]+', '', 'g') = table_name  -- only alphanumerics and underscore
  AND TRIM(BOTH '_' FROM table_name)                     = table_name  -- does not start or end with underscore
  AND TRIM(LEADING '01234567879' FROM table_name)        = table_name  -- does not start with a number

{% endmacro %}