{#/*
Test that two models contain identical content, for the specified ID ranges.
Based on https://github.com/fishtown-analytics/dbt-utils/blob/master/macros/schema_tests/equality.sql

Why would you need this?:
    If you are building a detailed dbt project, you might have seed data to do a series of tests on the models.
    However, to thoroughly test each model they may require a variety of model data to confirm edge cases.
    If an equality test was used, the test would have to be updated each time a potentially related piece of data was changed, which is impractical.
    Instead, define a small set of data that is guaranteed to not be modified or treated weirdly for unrelated edge-case tests and test on just that data.

Arguments:
    model: table model (not needed when called from schema.yml)
    compare_to: the model reference to compare to
    id_column: the name of the column on both models to check id ranges on
    min: the minimum allowable id value
    max: the maximum allowable id value
    exclude_timestamp: if true, any column that follows the timestamp naming convention will be excluded from comparison
*/#}
{% macro test_equality_for_range(model, compare_to, id_column, min, max, exclude_timestamp = false) %}

-- setup

{% set schema = model.schema %}
{% set model_a_name = model.name %}

{% set dest_columns = adapter.get_columns_in_table(schema, model_a_name) %}

{% set dest_columns_filtered = [] %}
{% for col in dest_columns if not exclude_timestamp or not col.name.endswith('_data_pipeline_timestamp') %}
  {{ dest_columns_filtered.append(col.quoted) | default('', true) }}
{% endfor %}

{% set dest_cols_csv = dest_columns_filtered | join(', ') %}


-- core SQL

with all_a as (

    select * from {{ model }}

),

a as (

    select * 
    from all_a
    where {{ id_column }} >= {{ min }}
      and {{ id_column }} <= {{ max }}

),

all_b as (

    select * from {{ compare_to }}

),

b as (

    select * 
    from all_b
    where {{ id_column }} >= {{ min }}
      and {{ id_column }} <= {{ max }}

),

a_minus_b as (

    select {{dest_cols_csv}} from a
    except
    select {{dest_cols_csv}} from b

),

b_minus_a as (

    select {{dest_cols_csv}} from b
    except
    select {{dest_cols_csv}} from a

),

unioned as (

    select * from a_minus_b
    union all
    select * from b_minus_a

),

final as (

    select (select count(*) from unioned) +
        (select abs(
            (select count(*) from a_minus_b) -
            (select count(*) from b_minus_a)
            ))
        as count

)

select count from final



{% endmacro %}
