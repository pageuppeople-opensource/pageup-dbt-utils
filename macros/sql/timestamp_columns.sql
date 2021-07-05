{#/*
    Generate aggregated timestamp columns.

    Accepts a params args of tablealias/column names.
    If a table is provided, the column used is 'model_timestamp'.
    Example usage:

      {{ timestamp_columns('foo', 'bar.column', 'baz') }}

    Result:

      GREATEST(foo.model_timestamp, bar.column, baz.model_timestamp) AS aggregated_model_timestamp
*/#}

{% macro timestamp_columns() -%}

GREATEST(
  {%- for column in varargs -%}
    {%- if column.find('.') == -1 -%}
      {{ column }}.{{ var("TIMESTAMP_SUFFIX", "model_timestamp") }}
    {%- else -%}
      {{ column }}
    {%- endif -%}
    {%- if not loop.last %}, {% endif -%}
  {%- endfor -%}
) {%- if kwargs['exclude_column_name'] != true %} AS {{ var("TIMESTAMP_SUFFIX", "model_timestamp") }}  {% endif -%}

{%- endmacro %}
