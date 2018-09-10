{% macro test_assert_has_version_false(model, column_name) %}

SELECT CASE 
           WHEN ({{ pageup_dbt_utils.test_has_version(model, column_name) }}) > 0 THEN 0
           ELSE 1
       END

{% endmacro %}
