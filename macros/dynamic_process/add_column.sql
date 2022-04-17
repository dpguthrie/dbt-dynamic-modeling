{% macro add_column(env, schema, table, column, data_type, comment='') %}

    {% set sql %}
    ALTER TABLE {{ env }}.{{ schema }}.{{ table }}
    ADD {{ column }} {{ data_type }} comment '{{ comment }}';
    {% endset %}

    {{ return(sql) }}

{% endmacro %}
