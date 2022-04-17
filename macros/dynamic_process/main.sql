{% macro main(env=None, product='award') %}

{# 

The data within rows returned from the `get_current_form_elements` macro can be accessed with the following:

    row[0] = tenant_id,
    row[1] = sqlized_tenant_id,
    row[2] = data_type,
    row[3] = cayuse_type,
    row[4] = comment,
    row[5] = id,
    row[6] = sqlized_id,
    row[7] = multiple_values,
    row[8] = max_timestamp
    row[9] = tenant_schema
    row[10] = has_column

#}

{% if execute %}

    {% set env = env or target.database %}

    -- Get current form elements from raw data
    {% set all_form_elements = get_current_form_elements(env) %}

    -- Distinct tenants in dataset
    {% set distinct_sqlized_tenant_ids = all_form_elements.columns[1].values_distinct() %}

    -- Get current client schemas
    {% set tenant_schemas = get_current_schemas(env) %}

    -- Loop through each distinct tenant, see if they have an existing schema
    {% for sqlized_tenant_id in distinct_sqlized_tenant_ids %}

        {% set tenant_schema = 'ANALYTICS_' + sqlized_tenant_id | upper %}

        {% if not tenant_schema in tenant_schemas %}
            {{ create_tenant_schema(tenant_schema) }}
            {{ create_inter_fact_table(product, tenant_schema, env) }}
        {% endif %}

    {% endfor %}

    {% set tenant_dict = dict() %}
    {% set bridge_dict = dict() %}
    {% set add_column_sql %}

    -- We need to have valid SQL if we don't find any columns to add
    SELECT 1;

    {% for row in all_form_elements %}

        -- If column not found in information_schema, then add
        {% if not row[10] -%}
            {{ inter_fact_add_column(env, row) }}
        {% endif -%}
        {% if row[1] not in tenant_dict.keys() -%}
            {% set _ = tenant_dict.update({row[1]: dict()}) -%}
            {% set _ = bridge_dict.update({row[1]: dict()}) -%}
        {% endif -%}
        {% set _ = tenant_dict[row[1]].update({row[6]: row}) -%}

        -- If cayuse_type or multiple_values or in standard_bridges, add to bridge rows
        {% if row[3] or row[7] or row[6] in var('standard_bridges') -%}
            {% set _ = bridge_dict[row[1]].update({row[6]: row}) -%}
        {% endif -%}

    {% endfor -%}

    {% endset -%}

    {% do run_query(add_column_sql) -%}

    -- Loop through each distinct tenant, merge data into inter_fact_award, create fact_award table, create bridge tables
    {% for sqlized_tenant_id in distinct_sqlized_tenant_ids %}

        {{ merge_into_inter_fact_table(env, product, sqlized_tenant_id, tenant_dict[sqlized_tenant_id]) }}
        {{ create_fact_table(env, product, sqlized_tenant_id, tenant_dict[sqlized_tenant_id], bridge_dict[sqlized_tenant_id]) }}
        {{ create_bridge_tables(env, product, sqlized_tenant_id, bridge_dict[sqlized_tenant_id]) }}

    {% endfor %}

{% endif %}

{% endmacro %}
