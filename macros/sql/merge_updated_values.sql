{#
Join the values of two tables together, taking values from the "new" table where they are updated, otherwise falling back
to the "old" table. This is useful for partial row updates as part of incrementals. (eg selective updates of pivoted rows)

The macro will merge rows using the following rules
 * every mapped column in the `new_table` has a matching updated flag column, denoted by a naming convention suffix.
 * A column in the `new_table` is considered to be updated if its matching updated flag is true (so updating to null is supported)
 * If a column in the `new_table` is not updated, then the existing value in the `old_table` is used
 * If the column also doesnt exist in the `old_table` (eg new row), then a default value is used
 * Designed to be used with incremental materializations. By default will only pass through `new_table` columns when
   `is_incremental()` is false.

Example existing model `old_rows`:

| id | col1 | col2 | col3 |
|----|------|------|------|
| 1  | foo  | bar  | baz  |
| 2  | NULL | re   | mi   |
| 3  | NULL | so   | la   |

Example updated rows `new_rows`:

| id | col1 | col2 | col3 | col1__is_updated | col2__is_updated | col3__is_updated | 
|----|------|------|------|------------------|------------------|------------------|
| 1  | NULL | NULL | NULL | FALSE            | TRUE             | FALSE            |
| 2  | do   | NULL | NULL | TRUE             | FALSE            | FALSE            |
| 4  | ti   | do   | NULL | TRUE             | TRUE             | FALSE            |


Example usage:

SELECT new_rows.id
       {{ pageup_dbt_utils.merge_updated_values(
           new_table='new_rows',
           old_table='old_rows',
           unique_key='id',
           merge_column_names=['col1','col2','col3']
       ) }}
FROM new_rows
{%- if is_incremental() %}
LEFT JOIN {{ this }} AS old_rows on old_rows.id = new_rows.id
{% endif -%}

Expected resulting model after update:

| id | col1 | col2 | col3 |
|----|------|------|------|
| 1  | foo  | NULL | baz  |
| 2  | do   | re   | mi   |
| 3  | NULL | so   | la   |
| 4  | ti   | do   | NULL |

Arguments:
    new_table: The table alias of the table containing the newly updated rows
    old_table: The table alias of the table containing the existing rows. Usually `{{ this }}`
    unique_key: The joining key on the `old_table`, for detecting if the row exists
    merge_column_names: The array of columns to be merged. This could be the output of the `dbt_utils.get_column_values` macro
    update_flag_suffix: The suffix that is added to identify update flag columns. Default `__is_updated`
    else_value: Value to use if a column does not exist in new or old tables. Default `NULL`
    do_nothing_if_not_incremental: If `is_incremental()` is true, then skip logic and pass through `new_table`. Default true
#}

{% macro merge_updated_values(new_table,
                              old_table,
                              unique_key,
                              merge_column_names,
                              update_flag_suffix='__is_updated',
                              else_value='NULL',
                              do_nothing_if_not_incremental=true) %}

  {%- for column in merge_column_names -%}
    {%- if do_nothing_if_not_incremental && not is_incremental() -%}
      {{ new_table }}.{{ column }}
    {%- else -%}
      CASE
          WHEN {{ new_table }}.{{ column }}{{ update_flag_suffix }} = TRUE THEN {{ new_table }}.{{ column }}
          WHEN {{ old_table }}.{{ unique_key }} != NULL THEN {{ old_table }}.{{ column }}
          ELSE {{ else_value }}
      END AS {{ column }}
    {% endif %}
    {%- if not loop.last %},{% endif %}
  {% endfor -%}
{%- endmacro -%}
