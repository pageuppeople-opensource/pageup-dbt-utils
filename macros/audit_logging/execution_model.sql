{% macro get_execution_model_relation() %}
    {%- set execution_model_table =
        api.Relation.create(
            identifier='execution_model',
            schema='dbt',
            type='table'
        ) -%}
    {{ return(execution_model_table) }}
{% endmacro %}

{% macro log_execution_model_event(result) %}

    insert into {{ pageup_dbt_utils.get_execution_model_relation() }} (
        execution_id,
        last_updated_on,
        status,
        model_schema,
        model_name,
        error_message,
        skipped,
        fail,
        execution_time
        )

    values (
        '{{ invocation_id }}'::uuid,
        {{dbt_utils.current_timestamp_in_utc()}},
        '{{ result.status }}',
        '{{ result.node.schema }}',
        '{{ result.node.table  }}',
        '{{ result.error }}',
        '{{ result.skip }}',
        '{{ result.fail }}',
        '{{ result.execution_time }}'
        )

{% endmacro %}


{% macro create_execution_model_log_table() %}

    create table if not exists {{ pageup_dbt_utils.get_execution_model_relation() }}
    (
        execution_model_id  uuid PRIMARY KEY NOT NULL execution_model_id NOT NULL DEFAULT uuid_generate_v1(),
        created_on          {{dbt_utils.type_timestamp()}} NOT NULL DEFAULT current_timestamp,
        execution_id        uuid NOT NULL,
        last_updated_on     {{dbt_utils.type_timestamp()}} NOT NULL,
        status              varchar(512) NOT NULL,
        model_schema        varchar(512) NOT NULL,
        model_name          varchar(512) NOT NULL,
        error_message       varchar(1024),
        skipped             boolean NOT NULL DEFAULT FALSE,
        fail                boolean NOT NULL DEFAULT FALSE,
        execution_time      integer NOT NULL
    )

{% endmacro %}
