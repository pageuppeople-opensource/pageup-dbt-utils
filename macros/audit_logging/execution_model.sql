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
        execution_model_id,
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
        fn_uuid()::text,
        '{{ invocation_id }}'::text,
        {{dbt_utils.current_timestamp_in_utc()}},
        {% if variable != None %}'{{ result.status }}'{% else %} 'ERROR UNKNOWN' {% endif %},
        '{{ result.node.schema }}',
        '{{ result.node.name  }}',
        {% if result.error != None %}'{{ result.error }}'{% else %} null {% endif %},
        {% if result.skip != None %}{{ result.skip }}{% else %} FALSE {% endif %},
        {% if result.fail != None %}{{ result.fail }}{% else %} FALSE {% endif %},
        '{{ result.execution_time }}'
        )

{% endmacro %}


{% macro create_execution_model_log_table() %}

    create table if not exists {{ pageup_dbt_utils.get_execution_model_relation() }}
    (
        execution_model_id  varchar(250) PRIMARY KEY NOT NULL,
        created_on          {{dbt_utils.type_timestamp()}} NOT NULL DEFAULT current_timestamp,
        execution_id        varchar(250) NOT NULL,
        last_updated_on     {{dbt_utils.type_timestamp()}} NOT NULL,
        status              varchar(512) NOT NULL,
        model_schema        varchar(512) NOT NULL,
        model_name          varchar(512) NOT NULL,
        error_message       varchar(1024),
        skipped             boolean NOT NULL DEFAULT FALSE,
        fail                boolean NOT NULL DEFAULT FALSE,
        execution_time      float NOT NULL
    )

{% endmacro %}
