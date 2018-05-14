{% macro test_assert_has_timestamp_false(model, arg) %}

SELECT CASE 
           WHEN ({{ pageup_dbt_utils.test_has_timestamp(model, arg) }}) > 0 THEN 0
           ELSE 1
       END

{% endmacro %}
