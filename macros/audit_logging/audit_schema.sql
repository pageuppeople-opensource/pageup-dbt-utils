{% macro create_audit_schema() %}
    {% do adapter.create_schema(target.database, "dbt") %}
{% endmacro %}
