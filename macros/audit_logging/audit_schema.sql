{% macro create_audit_schema() %}
    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
    {% do adapter.create_schema(api.Relation.create(database=target.database, schema="dbt")) %};
{% endmacro %}
