{% macro inter_fact_add_column(env, row) %}

    {% set data_type = get_data_type(row) %}
    {% set cayuse_type = row[3] %}
    {% set comment = row[4] %}
    {% set sqlized_id = row[6] %}
    {% set multiple_values = row[7] %}
    {% set tenant_schema = row[9] %}

    {{ _standard_add_column(
        env,
        tenant_schema,
        'INTER_FACT_AWARD',
        sqlized_id,
        data_type,
        comment
    ) }}

{% endmacro %}

{% macro _standard_add_column(env, schema, table, column, data_type, comment='') %}

    {% set sql %}
    ALTER TABLE {{ env }}.{{ schema }}.{{ table }}
    ADD {{ column }} {{ data_type }} comment '{{ comment }}';
    {% endset %}

    {{ return(sql) }}


{% endmacro %}
