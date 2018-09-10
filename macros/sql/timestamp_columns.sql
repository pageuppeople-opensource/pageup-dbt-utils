{#/*
    Generate aggregated timestamp columns.

    Accepts a params args of tablealias/column names.
    If a table is provided, the column used is 'data_pipeline_timestamp'.
    Example usage:
    
      {{ timestamp_columns('foo', 'bar.column', 'baz') }}

    Result:

      GREATEST(foo.data_pipeline_timestamp, bar.column, baz.data_pipeline_timestamp) AS aggregated_data_pipeline_timestamp
*/#}

{% macro timestamp_columns() -%}

GREATEST(
  {%- for column in varargs -%}
    {%- if column.find('.') == -1 -%}
      {{ column }}.data_pipeline_timestamp
    {%- else -%}
      {{ column }}
    {%- endif -%}
    {%- if not loop.last %}, {% endif -%}
  {%- endfor -%}
) AS aggregated_data_pipeline_timestamp

{%- endmacro %}