{#/*
Pivot values from rows to columns, using a mapping table.
Based on the pivot utility in dbt-utils https://github.com/fishtown-analytics/dbt-utils

A mapping table can be used when you are pivoting on a selection of foreign keys. The mapping table should
have a column for every key to map/pivot and a column containing what the pivoted column name should be. 
Function assumes that each key value only maps to a single column value.

Example:

    Input: `public.test`

    | size | color_id |
    |------+----------|
    | S    | 1        |
    | S    | 2        |
    | S    | 1        |
    | M    | 1        |

    Mapping: `public.mapping`

    | color_id | color |
    |----------+-------|
    | 1        | red   |
    | 2        | blue  |
*/
    select
      size,
      {{ pageup_dbt_utils.mapped_pivot(column:        'color_id', 
                                       mapping_table: ref('public.mapping'),
                                       mapping_key:   'color_id',
                                       mapping_value: 'color') }}
    from public.test
    group by size
/*
    Output:

    | size | red | blue |
    |------+-----+------|
    | S    | 2   | 1    |
    | M    | 1   | 0    |

Arguments:
    column: Column name, required
    mapping_table:  A model `ref`, or a schema.table string for the table to query (Required)
    mapping_key: The column name in the mapping table that maps to the row values to be pivoted.
    mapping_value: The column name in the mapping table that contains the final column names of the row values to be pivoted.
    agg: SQL aggregation function, default is sum
    cmp: SQL value comparison, default is =
    then_value: Value to use if comparison succeeds, default is 1
    else_value: Value to use if comparison fails, default is 0
*/#}

{% macro mapped_pivot(column,
                      mapping_table,
                      mapping_key,
                      mapping_value,
                      agg='sum',
                      cmp='=',
                      then_value=1,
                      else_value=0) %}

  {%- call statement('get_pivot_mapping', fetch_result=True) %}

      SELECT DISTINCT
             {{ mapping_key }}   AS pivot_key,
             {{ mapping_value }} AS pivot_value
      FROM   {{ mapping_table }}
      ORDER BY 2

  {% endcall -%}

  {%- set value_list = load_result('get_pivot_mapping')['data'] -%}

  {%- for kvp in value_list -%}
    {{ agg }}(
        CASE
            WHEN {{ column }} {{ cmp }} '{{ kvp[0] }}' THEN {{ then_value }}
            ELSE {{ else_value }}
        END
    ) AS {{ kvp[1] }}
    {%- if not loop.last %},{% endif %}
  {% endfor %}
{% endmacro %}
