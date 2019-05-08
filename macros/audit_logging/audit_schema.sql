{% macro create_audit_schema() %}
    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
    {{ adapter.create_schema(target.database, "dbt") }};
{% endmacro %}
