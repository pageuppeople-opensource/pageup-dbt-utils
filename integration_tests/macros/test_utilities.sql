{% macro test_assert_equal(model, actual, expected) %}

select count(*) from {{ model }} where {{ actual }} != {{ expected }}

{% endmacro %}