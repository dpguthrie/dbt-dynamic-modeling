{% macro main(env=None, product='award') %}

{% if execute %}

    {% set env = env or target.database %}

    {% do log('Database: ' ~ env, info=True) %}

    -- Get current form elements from raw data
    {% set all_form_elements = get_current_form_elements(env, product) %}

    {% do log('All distinct form elements for ' ~ product ~ ' queried!', info=True) %}

    -- Distinct tenants in dataset
    {% set distinct_sqlized_tenant_ids = all_form_elements.columns[1].values_distinct() %}

    {% do log('Distinct tenants: ' ~ distinct_sqlized_tenant_ids | join(', '), info=True) %}

    -- Get current client schemas
    {% set tenant_schemas = get_current_schemas(env) %}

    {% do log('Current tenant schemas: ' ~ tenant_schemas | join(', '), info=True) %}

    -- Loop through each distinct tenant, see if they have an existing schema
    {% for sqlized_tenant_id in distinct_sqlized_tenant_ids %}

        {% set tenant_schema = 'ANALYTICS_' + sqlized_tenant_id | upper %}

        {% if not tenant_schema in tenant_schemas %}
            {% do log('Creating tenant schema and inter_fact_table for ' ~ sqlized_tenant_id, info=True) %}
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
            {% set data_type = get_data_type(row) %}
            {{ add_column(env, row[9], 'INTER_FACT_AWARD', row[6], data_type, row[4]) }}
            {% do log(row[6] ~ ' (' ~ data_type ~ ') column added in ' ~ row[9] ~ '.INTER_FACT_' ~ product | upper, info=True) %}
        {% endif -%}
        {% if row[1] not in tenant_dict.keys() -%}
            {% set _ = tenant_dict.update({row[1]: dict()}) -%}
            {% set _ = bridge_dict.update({row[1]: dict()}) -%}
        {% endif -%}
        {% set _ = tenant_dict[row[1]].update({row[6]: row}) -%}

        -- If cayuse_type or multiple_values or in standard_bridges, add to bridge dict
        {% if row[3] or row[7] or row[6] in var('standard_bridges') -%}
            {% set _ = bridge_dict[row[1]].update({row[6]: row}) -%}
        {% endif -%}

    {% endfor -%}

    {% endset -%}

    {% do run_query(add_column_sql) -%}

    -- Loop through each distinct tenant, merge data into inter_fact_award, create fact_award table, create bridge tables
    {% for sqlized_tenant_id in distinct_sqlized_tenant_ids %}

        {{ merge_into_inter_fact_table(env, product, sqlized_tenant_id, tenant_dict[sqlized_tenant_id]) }}
        {% do log('Successful merge into ANALYTICS_' ~ sqlized_tenant_id | upper ~ '.INTER_FACT_AWARD!', info=True) %}
        {{ create_fact_table(env, product, sqlized_tenant_id, tenant_dict[sqlized_tenant_id], bridge_dict[sqlized_tenant_id]) }}
        {% do log('Successfully created ANALYTICS_' ~ sqlized_tenant_id | upper ~ '.FACT_AWARD!', info=True) %}
        {{ create_bridge_tables(env, product, sqlized_tenant_id, bridge_dict[sqlized_tenant_id]) }}
        {% do log('All bridge tables created for: ' ~ sqlized_tenant_id | upper, info=True) %}

    {% endfor %}

{% endif %}

{% endmacro %}
