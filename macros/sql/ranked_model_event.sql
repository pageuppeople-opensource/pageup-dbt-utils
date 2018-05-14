{#/*
Selects the specified model and ranks the event versions in descending order.

Arguments:
    identifier_column: Column name, required
*/#}

{% macro ranked_model_event(identifier_column) %}

    {{ identifier_column }},
    event_version,
    event_blob,
    ROW_NUMBER() OVER (PARTITION BY {{ identifier_column }} ORDER BY event_version DESC) AS event_version_rank

{% endmacro %}
