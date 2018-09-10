{% macro test_assert_has_timestamp_false(model, column_name) %}

SELECT CASE 
           WHEN ({{ pageup_dbt_utils.test_has_timestamp(model, column_name) }}) > 0 THEN 0
           ELSE 1
       END

{% endmacro %}
