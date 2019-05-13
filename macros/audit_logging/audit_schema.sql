{% macro create_audit_schema() %}
    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
    {% do adapter.create_schema(target.database, "dbt") %};
{% endmacro %}
