{% macro get_execution_model_relation() %}
    {%- set execution_model_table =
        api.Relation.create(
            identifier='execution_model',
            schema='dbt',
            type='table'
        ) -%}
    {{ return(execution_model_table) }}
{% endmacro %}

{% macro log_execution_model_event(status, schema_name, model_name) %}

    insert into {{ logging.get_execution_model_relation() }} (
        execution_id,
        last_updated_on,
        status,


        model_schema,
        model_name
        )

    values (
        '{{ invocation_id }}',
        {{dbt_utils.current_timestamp_in_utc()}},
        '{{ status }}'
        '{{ schema_name }}',
        '{{ model_name }}'
        )

{% endmacro %}


{% macro create_execution_model_log_table() %}

    create table if not exists {{ logging.get_execution_model_relation() }}
    (
        execution_model_id  uuid PRIMARY KEY NOT NULL DEFAULT uuid_generate_v1(),
        created_on          {{dbt_utils.type_timestamp()}} NOT NULL DEFAULT current_timestamp,
        execution_id        uuid NOT NULL,
        last_updated_on     {{dbt_utils.type_timestamp()}} NOT NULL,
        status              varchar(512) NOT NULL,
        model_schema        varchar(512) NOT NULL,
        model_name          varchar(512) NOT NULL
    )

{% endmacro %}

{% macro log_model_start_event() %}
    {{logging.log_execution_model_event(
        'started', this.schema, this.name
        )}}
{% endmacro %}


{% macro log_model_end_event() %}
    {{logging.log_execution_model_event(
        'completed', this.schema, this.name
        )}}
{% endmacro %}
