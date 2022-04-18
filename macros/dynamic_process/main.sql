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

    {% set tenant_dict = dict() %} -- Will be used to ensure we have every column to select from
    {% set bridge_dict = dict() %} -- Will be used to create bridge tables
    {% set column_dict = dict() %} -- Will be used to create columns in inter_fact table

    {% for row in all_form_elements %}

        -- Ensure sqlized_tenant_id exists in dictionaries
        {% if row[1] not in tenant_dict.keys() -%}
            {% set _ = tenant_dict.update({row[1]: dict()}) -%}
            {% set _ = bridge_dict.update({row[1]: dict()}) -%}
        {% endif -%}

        -- If column not found in information_schema, then add
        {% if not row[10] -%}
            {% if row[1] not in column_dict.keys() -%}
                {% set _ = column_dict.update({row[1]: []}) -%}
            {% endif %}
            {% set sql %}
            {{ row[6] }} {{ get_data_type(row) }} comment '{{ row[4] }}'
            {% endset %}
            {{ column_dict[row[1]].append(sql) }}
        {% endif -%}

        {% set _ = tenant_dict[row[1]].update({row[6]: row}) -%}

        -- If cayuse_type or multiple_values or in standard_bridges, add to bridge dict
        {% if row[3] or row[7] or row[6] in var('standard_bridges') -%}
            {% set _ = bridge_dict[row[1]].update({row[6]: row}) -%}
        {% endif -%}

    {% endfor -%}

    -- Add columns to INTER_FACT_{{ product }} table
    {% for sqlized_tenant_id, column_list in column_dict.items() %}
        {% set tenant_schema = 'ANALYTICS_' + sqlized_tenant_id | upper %}
        {% set sql %}
        ALTER TABLE {{ env }}.{{ tenant_schema }}.INTER_FACT_{{ product | upper }}
        ADD {{ column_list | join(', ') }}
        {% endset %}

        {% do run_query(sql) %}
        {% do log(column_list | length ~ ' columns added in ' ~ tenant_schema ~ '.INTER_FACT_' ~ product | upper, info=True) -%}
    {% endfor %}

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
