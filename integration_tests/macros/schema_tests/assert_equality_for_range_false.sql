{% macro test_assert_equality_for_range_false(model, compare_to, id_column, min, max) %}

SELECT CASE 
           WHEN ({{ pageup_dbt_utils.test_equality_for_range(model, compare_to, id_column, min, max) }}) > 0 THEN 0
           ELSE 1
       END

{% endmacro %}
