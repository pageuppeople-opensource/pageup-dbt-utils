{% macro get_execution_relation() %}
    {%- set execution_table =
        api.Relation.create(
            identifier='execution',
            schema='dbt',
            type='table'
        ) -%}
    {{ return(execution_table) }}
{% endmacro %}


{% macro log_execution_event(status) %}


{% endmacro %}


{% macro create_execution_log_table() %}

    create table if not exists {{ pageup_dbt_utils.get_execution_relation() }}
    (
        execution_id        uuid PRIMARY KEY NOT NULL,
        created_on          {{dbt_utils.type_timestamp()}} NOT NULL DEFAULT current_timestamp,
        last_updated_on     {{dbt_utils.type_timestamp()}} NOT NULL,
        is_full_refresh     boolean NOT NULL,
        status              varchar(512) NOT NULL
    )

{% endmacro %}


{% macro log_execution_start_event() %}
    insert into {{ pageup_dbt_utils.get_execution_relation() }} (
        execution_id,
        last_updated_on,
        is_full_refresh,
        status
        )

    values (
        '{{ invocation_id }}'::uuid,
        {{dbt_utils.current_timestamp_in_utc()}},
        {{ should_full_refresh() }},
        'started'
        )
{% endmacro %}


{% macro log_execution_end_event() %}
    UPDATE {{ pageup_dbt_utils.get_execution_relation() }}
    SET (
        last_updated_on,
        status
        )
    = (
        {{dbt_utils.current_timestamp_in_utc()}},
        'completed'
        )
    WHERE execution_id='{{ invocation_id }}'::uuid;
    {% for result in results -%}
        {{ pageup_dbt_utils.log_execution_model_event(result) }};
    {% endfor %}
{% endmacro %}
