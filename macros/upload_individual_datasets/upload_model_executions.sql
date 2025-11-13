{% macro upload_model_executions(models) -%}
    {{ return(adapter.dispatch('get_model_executions_dml_sql', 'dbt_artifacts')(models)) }}
{%- endmacro %}

{% macro default__get_model_executions_dml_sql(models) -%}
    {% if models != [] %}
        {% set model_execution_values %}
        select
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(1) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(2) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(3) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(4) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(5) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(6) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(7) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(8) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(9) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(10) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(11) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(12) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(13) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(14) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(15) }},
            {{ adapter.dispatch('parse_json', 'dbt_artifacts')(adapter.dispatch('column_identifier', 'dbt_artifacts')(16)) }}

        from values
        {% for model in models -%}
            (
                '{{ invocation_id }}', {# command_invocation_id #}
                '{{ model.node.unique_id }}', {# node_id #}
                '{{ run_started_at }}', {# run_started_at #}

                {% set config_full_refresh = model.node.config.full_refresh %}
                {% if config_full_refresh is none %}
                    {% set config_full_refresh = flags.FULL_REFRESH %}
                {% endif %}
                '{{ config_full_refresh }}', {# was_full_refresh #}

                '{{ model.thread_id }}', {# thread_id #}
                '{{ model.status }}', {# status #}

                {% set compile_started_at = (model.timing | selectattr("name", "eq", "compile") | first | default({}))["started_at"] %}
                {% if compile_started_at %}'{{ compile_started_at }}'{% else %}null{% endif %}, {# compile_started_at #}
                {% set query_completed_at = (model.timing | selectattr("name", "eq", "execute") | first | default({}))["completed_at"] %}
                {% if query_completed_at %}'{{ query_completed_at }}'{% else %}null{% endif %}, {# query_completed_at #}

                {{ model.execution_time }}, {# total_node_runtime #}
                null, -- rows_affected not available {# Only available in Snowflake & BigQuery #}
                '{{ model.node.config.materialized }}', {# materialization #}
                '{{ model.node.schema }}', {# schema #}
                '{{ model.node.name }}', {# name #}
                '{{ model.node.alias }}', {# alias #}
                '{{ model.message | replace("\\", "\\\\") | replace("'", "\\'") | replace('"', '\\"') }}', {# message #}
                '{{ tojson(model.adapter_response) | replace("\\", "\\\\") | replace("'", "\\'") | replace('"', '\\"') }}' {# adapter_response #}
            )
            {%- if not loop.last %},{%- endif %}
        {%- endfor %}
        {% endset %}
        {{ model_execution_values }}
    {% else %}
        {{ return("") }}
    {% endif %}
{% endmacro -%}

{% macro bigquery__get_model_executions_dml_sql(models) -%}
    {% if models != [] %}
        {% set model_execution_values %}
        {% for model in models -%}
            (
            '{{ invocation_id }}', {# command_invocation_id #}
            '{{ model.node.unique_id }}', {# node_id #}
            '{{ run_started_at }}', {# run_started_at #}

            {% set config_full_refresh = model.node.config.full_refresh %}
            {% if config_full_refresh is none %}
                {% set config_full_refresh = flags.FULL_REFRESH %}
            {% endif %}
            {{ config_full_refresh }}, {# was_full_refresh #}

            '{{ model.thread_id }}', {# thread_id #}
            '{{ model.status }}', {# status #}

            {% set compile_started_at = (model.timing | selectattr("name", "eq", "compile") | first | default({}))["started_at"] %}
            {% if compile_started_at %}'{{ compile_started_at }}'{% else %}null{% endif %}, {# compile_started_at #}
            {% set query_completed_at = (model.timing | selectattr("name", "eq", "execute") | first | default({}))["completed_at"] %}
            {% if query_completed_at %}'{{ query_completed_at }}'{% else %}null{% endif %}, {# query_completed_at #}

            {{ model.execution_time }}, {# total_node_runtime #}
            safe_cast('{{ model.adapter_response.rows_affected }}' as int64),
            safe_cast('{{ model.adapter_response.bytes_processed }}' as int64),
            '{{ model.node.config.materialized }}', {# materialization #}
            '{{ model.node.schema }}', {# schema #}
            '{{ model.node.name }}', {# name #}
            '{{ model.node.alias }}', {# alias #}
            '{{ model.message | replace("\\", "\\\\") | replace("'", "\\'") | replace('"', '\\"') | replace("\n", "\\n") }}', {# message #}
            {{ adapter.dispatch('parse_json', 'dbt_artifacts')(tojson(model.adapter_response) | replace("\\", "\\\\") | replace("'", "\\'") | replace('"', '\\"')) }} {# adapter_response #}
            )
            {%- if not loop.last %},{%- endif %}
        {%- endfor %}
        {% endset %}
        {{ model_execution_values }}
    {% else %}
        {{ return("") }}
    {% endif %}
{%- endmacro %}

{% macro snowflake__get_model_executions_dml_sql(models) -%}
    {% if models != [] %}
        {% set model_execution_values %}
        select
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(1) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(2) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(3) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(4) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(5) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(6) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(7) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(8) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(9) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(10) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(11) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(12) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(13) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(14) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(15) }},
            {{ adapter.dispatch('parse_json', 'dbt_artifacts')(adapter.dispatch('column_identifier', 'dbt_artifacts')(16)) }}
        from values
        {% for model in models -%}
            (
                '{{ invocation_id }}', {# command_invocation_id #}
                '{{ model.node.unique_id }}', {# node_id #}
                '{{ run_started_at }}', {# run_started_at #}

                {% set config_full_refresh = model.node.config.full_refresh %}
                {% if config_full_refresh is none %}
                    {% set config_full_refresh = flags.FULL_REFRESH %}
                {% endif %}
                '{{ config_full_refresh }}', {# was_full_refresh #}

                '{{ model.thread_id }}', {# thread_id #}
                '{{ model.status }}', {# status #}

                {% set compile_started_at = (model.timing | selectattr("name", "eq", "compile") | first | default({}))["started_at"] %}
                {% if compile_started_at %}'{{ compile_started_at }}'{% else %}null{% endif %}, {# compile_started_at #}
                {% set query_completed_at = (model.timing | selectattr("name", "eq", "execute") | first | default({}))["completed_at"] %}
                {% if query_completed_at %}'{{ query_completed_at }}'{% else %}null{% endif %}, {# query_completed_at #}

                {{ model.execution_time }}, {# total_node_runtime #}
                try_cast('{{ model.adapter_response.rows_affected }}' as int), {# rows_affected #}
                '{{ model.node.config.materialized }}', {# materialization #}
                '{{ model.node.schema }}', {# schema #}
                '{{ model.node.name }}', {# name #}
                '{{ model.node.alias }}', {# alias #}
                '{{ model.message | replace("\\", "\\\\") | replace("'", "\\'") | replace('"', '\\"') }}', {# message #}
                '{{ tojson(model.adapter_response) | replace("\\", "\\\\") | replace("'", "\\'") | replace('"', '\\"') }}' {# adapter_response #}
            )
            {%- if not loop.last %},{%- endif %}
        {%- endfor %}
        {% endset %}
        {{ model_execution_values }}
    {% else %}
        {{ return("") }}
    {% endif %}
{% endmacro -%}

{% macro postgres__get_model_executions_dml_sql(models) -%}
    {% if models != [] %}
        {% set model_execution_values %}
        {% for model in models -%}
            (
                '{{ invocation_id }}', {# command_invocation_id #}
                '{{ model.node.unique_id }}', {# node_id #}
                '{{ run_started_at }}', {# run_started_at #}

                {% set config_full_refresh = model.node.config.full_refresh %}
                {% if config_full_refresh is none %}
                    {% set config_full_refresh = flags.FULL_REFRESH %}
                {% endif %}
                {{ config_full_refresh }}, {# was_full_refresh #}

                '{{ model.thread_id }}', {# thread_id #}
                '{{ model.status }}', {# status #}

                {% set compile_started_at = (model.timing | selectattr("name", "eq", "compile") | first | default({}))["started_at"] %}
                {% if compile_started_at %}'{{ compile_started_at }}'{% else %}null{% endif %}, {# compile_started_at #}
                {% set query_completed_at = (model.timing | selectattr("name", "eq", "execute") | first | default({}))["completed_at"] %}
                {% if query_completed_at %}'{{ query_completed_at }}'{% else %}null{% endif %}, {# query_completed_at #}

                {{ model.execution_time }}, {# total_node_runtime #}
                null, {# rows_affected #}
                '{{ model.node.config.materialized }}', {# materialization #}
                '{{ model.node.schema }}', {# schema #}
                '{{ model.node.name }}', {# name #}
                '{{ model.node.alias }}', {# alias #}
                $${{ model.message }}$$, {# message #}
                $${{ tojson(model.adapter_response) }}$$ {# adapter_response #}
            )
            {%- if not loop.last %},{%- endif %}
        {%- endfor %}
        {% endset %}
        {{ model_execution_values }}
    {% else %}
        {{ return("") }}
    {% endif %}
{%- endmacro %}

{% macro athena__get_model_executions_dml_sql(models) -%}
    {% if models != [] %}
        {% set model_execution_values %}
        select
            cast(col1 as string) as command_invocation_id,
            cast(col2 as string) as node_id,
            cast(col3 as string) as run_started_at,
            cast(col4 as boolean) as was_full_refresh,
            cast(col5 as string) as thread_id,
            cast(col6 as string) as status,
            cast(col7 as string) as compile_started_at,
            cast(col8 as string) as query_completed_at,
            cast(col9 as double) as total_node_runtime,
            cast(col10 as integer) as rows_affected,
            cast(col11 as string) as materialization,
            cast(col12 as string) as schema,
            cast(col13 as string) as name,
            cast(col14 as string) as alias,
            cast(col15 as string) as message,
            cast(col16 as string) as adapter_response
        from values
        {% for model in models -%}
            (
                '{{ invocation_id }}', {# command_invocation_id #}
                '{{ model.node.unique_id }}', {# node_id #}
                '{{ run_started_at }}', {# run_started_at #}


                {% set config_full_refresh = model.node.config.full_refresh %}
                {% if config_full_refresh is none %}
                    {% set config_full_refresh = flags.FULL_REFRESH %}
                {% endif %}
                {{ config_full_refresh }}, {# was_full_refresh #}

                '{{ model.thread_id }}', {# thread_id #}
                '{{ model.status }}', {# status #}

                {% set compile_started_at = (model.timing | selectattr("name", "eq", "compile") | first | default({}))["started_at"] %}
                {% if compile_started_at %}
                    '{{ compile_started_at }}' {# compile_started_at #}
                {% else %}
                    null {# compile_started_at #}
                {% endif %},
                {% set query_completed_at = (model.timing | selectattr("name", "eq", "execute") | first | default({}))["completed_at"] %}
                {% if query_completed_at %}
                    '{{ query_completed_at }}' {# query_completed_at #}
                {% else %}
                    null
                {% endif %},
                {{ model.execution_time }}, {# total_node_runtime #}
                {{model.adapter_response.rows_affected}}, {# rows_affected #}
                '{{ model.node.config.materialized }}', {# materialization #}
                '{{ model.node.schema }}', {# schema #}
                '{{ model.node.name }}', {# name #}
                '{{ model.node.alias }}', {# alias #}
                '{{ model.message | replace("\\", "\\\\") | replace("'", "") | replace('"', "") | replace("\n", "\\n") }}', {# message #}
                '{{ tojson(model.adapter_response) }}' {# adapter_response #}
            )
            {%- if not loop.last %},{%- endif %}
        {%- endfor %}
        {% endset %}
        {{ model_execution_values }}
    {% else %}
        {{ return("") }}
    {% endif %}
{%- endmacro %}