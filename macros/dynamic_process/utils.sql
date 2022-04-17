{% macro get_data_type(row) %}

{% set data_type = var('data_types').get(row[2], 'varchar') %}

{% if row[3] or row[7] or row[6] in var('standard_bridges') %}
    {% set data_type = 'variant' %}
{% endif %}

{{ return(data_type) }}

{% endmacro %}