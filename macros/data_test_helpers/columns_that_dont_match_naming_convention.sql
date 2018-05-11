{#/*
Returns the names of all columns in all tables in target schema that do not match required naming convention
*/#}
{% macro columns_that_dont_match_naming_convention() %}

WITH column_names AS (
    SELECT table_schema,
           table_name,
           column_name
    FROM   information_schema.columns
    WHERE  table_schema LIKE '{{target.schema}}%'
)
SELECT table_schema || '.' || table_name || '.' || column_name
FROM   column_names
EXCEPT
SELECT table_schema || '.' || table_name || '.' || column_name
FROM   column_names
WHERE LOWER(column_name)                                  = column_name  -- only lowercase
  AND REGEXP_REPLACE(column_name, '[^a-z0-9_]+', '', 'g') = column_name  -- only alphanumerics and underscore
  AND TRIM(BOTH '_' FROM column_name)                     = column_name  -- does not start or end with underscore
  AND TRIM(LEADING '0123456789' FROM column_name)         = column_name  -- does not start with a number

{% endmacro %}