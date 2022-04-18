{% macro create_tenant_schema(tenant_schema, env=None) %}

    {% set env = env or target.database %}

    {% set sql %}
    use database {{ env }};
    create schema if not exists {{ tenant_schema }};
    {% endset %}

    {% do run_query(sql) %}

{% endmacro %}

{% macro create_inter_fact_table(product, tenant_schema, env=None) %}

    {% set env = env or target.database %}
    {% set common_columns = var('common_columns')[product] %}

    {% set sql %}
    create table if not exists {{ env }}.{{ tenant_schema }}.inter_fact_{{ product }} (
        {% for col, data_type in common_columns.items() -%}
        {{ col }} {{ data_type }}{% if not loop.last %}, {% endif -%}
        {% endfor -%}
    );
    {% endset %}

    {% do run_query(sql) %}

{% endmacro %}

{% macro merge_into_inter_fact_table(env, product, sqlized_tenant_id, tenant_dict) %}

    {% set tenant_schema = 'ANALYTICS_' + sqlized_tenant_id | upper %}
    {% set common_columns = var('common_columns')[product].keys() %}

    {% set sql %}
    merge into {{ env }}.{{ tenant_schema }}.inter_fact_{{ product }} as target
    using (
        with raw_data as (
            select parse_json(raw) as json
            from {{ env }}.products.{{ product }}
            qualify row_number() over (
                partition by json:awardId
                order by json:modificationNumber desc
            ) = 1
        ),

        parsed as (
            select
                json:tenantId::string as tenant_id,
                sha2(json:awardId::string) as award_key,
                sha2(json:tenantId::string) as tenant_key,
                json:awardId::string as award_id,
                json:awardNumber::string as award_number,
                json:projectId::string as project_id,
                json:projectNumber::string as project_number,
                json:projectTitle::string as project_title,
                json:adminTeamMembers::variant as admin_team_members,
                json:awardLegacyNumber::string as award_legacy_number,
                json:reviewStatus::string as review_status,
                json:createDate::timestamp_ntz as create_date,
                json:updateDate::timestamp_ntz as update_date,
                json:createdUser::string as create_user,
                json:updatedUser::string as update_user,
                json:statusName::string as status_name,
                json:statusKey::string as status_key,
                json:formElements as form_elements_json,
                sqlize_tenant_id(json:tenantId::string) as sqlized_tenant_id
            from raw_data
            where sqlized_tenant_id = '{{ sqlized_tenant_id }}'
        ),

        flattened as (
            select
                td.*,
                fe.value:answer as form_answer,
                fe.value:id as form_id
            from parsed td,
                lateral flatten(form_elements_json) fe
        )
        
        select
            {% for col in common_columns %}
            {{ col }},
            {% endfor %}
            {% for sqlized_id, row in tenant_dict.items() %}
            "'{{ row[5] }}'"::{{ get_data_type(row) }} as {{ sqlized_id }}{% if not loop.last %}, {% endif %}
            {% endfor %}
        from flattened
        pivot(max(form_answer) for form_id in (
            {% for row in tenant_dict.values() %}
            '{{ row[5] }}'{% if not loop.last %}, {% endif -%}
            {% endfor %}
        )) as p

    ) source
        on source.{{ product }}_id = target.{{ product }}_id
    when not matched then insert (
        {% for col in common_columns %}{{ col }},{% endfor -%}
        {% for sqlized_id in tenant_dict.keys() %}
        {{ sqlized_id }}{% if not loop.last %}, {% endif -%}
        {% endfor %}
    ) values (
        {% for col in common_columns %}source.{{ col }},{% endfor -%}
        {% for sqlized_id in tenant_dict.keys() %}
        source.{{ sqlized_id }}{% if not loop.last %},{% endif -%}
        {% endfor %}
    )
    when matched then update set
        {% for col in common_columns %}{{ col }} = source.{{ col }}, {% endfor -%}
        {% for sqlized_id in tenant_dict.keys() %}
        {{ sqlized_id }} = source.{{ sqlized_id }}{% if not loop.last %},{% endif -%}
        {% endfor %}
    ;
    {% endset %}

    {% do run_query(sql) %}

{% endmacro %}

{% macro create_fact_table(env, product, sqlized_tenant_id, tenant_dict, bridge_dict) %}

    {% set tenant_schema = 'ANALYTICS_' + sqlized_tenant_id | upper %}
    {% set common_columns = var('common_columns')[product].keys() %}
    {% set product_key = product + '_key' %}

    {% set sql %}
    create or replace table {{ env }}.{{ tenant_schema }}.fact_{{ product }} (
          {{ product_key }} varchar primary key
        {% for col in common_columns -%}
        {% if col != product_key -%}
        , {{ col }}
        {% endif -%}
        {% endfor -%}
        {% for bridge_col, row in bridge_dict.items() -%}
        , {{ bridge_col }}_group_key varchar unique comment '{{ row[4] }}'
        {% endfor -%}
        {% for col, row in tenant_dict.items() -%}
        {% if col not in bridge_dict.keys() -%}
        , {{ col }} comment '{{ row[4] }}'
        {% endif -%}
        {% endfor -%}
    ) as
    select
          {{ product_key }}
        {% for col in common_columns -%}
        {% if col != product_key -%}
        , {{ col }}
        {% endif -%}
        {% endfor -%}
        {% for bridge_col, row in bridge_dict.items() -%}
        , sha2(tenant_id || {{ product }}_id || {{ bridge_col }}) as {{ bridge_col }}_group_key
        {% endfor -%}
        {% for col in tenant_dict.keys() -%}
        {% if col not in bridge_dict.keys() -%}
        , {{ col }}
        {% endif -%}
        {% endfor -%}
    from {{ env }}.{{ tenant_schema }}.inter_fact_{{ product }};
    {% endset %}

    {% do run_query(sql) %}

{% endmacro %}

{% macro create_bridge_tables(env, product, sqlized_tenant_id, bridge_dict) %}

    {% set tenant_schema = 'ANALYTICS_' + sqlized_tenant_id | upper %}
    {% set common_columns = var('common_columns')[product].keys() %}
    {% set product_key = product + '_key' %}

    -- Currently, no checks here for the cayuse_type to create the different bridge table
    -- I didn't create the dim tables that would allow FK relationships
    {% set sql %}

    {% for bridge_col, row in bridge_dict.items() %}
        create or replace table {{ env }}.{{ tenant_schema }}.brdg_{{ product }}_{{ bridge_col }} (
            {{ bridge_col }}_group_key varchar unique comment '{{ row[4] }}',
            {{ bridge_col }}_value,
            brdg_{{ product }}_{{ bridge_col }}_key varchar primary key
        ) as
        with flattened as (
            select
                  sha2(tenant_id || {{ product }}_id || '{{ bridge_col }}') as {{ bridge_col }}_group_key
                , flow_field.value as {{ bridge_col }}_value
                , sha2(tenant_id || {{ product }}_id || {{ bridge_col }}_value) as brdg_{{ product }}_{{ bridge_col }}_key
            from {{ env }}.{{ tenant_schema }}.inter_fact_{{ product }},
                table (flatten(split(regexp_replace({{ row[6] }}, '\\\[|\\\]|( )', ''), ','))) flow_field
        )

        select * from flattened;

        alter table {{ env }}.{{ tenant_schema }}.brdg_{{ product }}_{{ bridge_col }}
        add foreign key ({{ bridge_col }}_group_key) references {{ env }}.{{ tenant_schema }}.fact_{{ product }}({{ bridge_col }}_group_key);

    {% endfor %}

    {% endset %}

    {% do run_query(sql) %}

{% endmacro %}

{% macro _create_cayuse_bridge_table(env, tenant_schema, product, bridge_col, row) %}

{% endmacro %}

{% macro _create_multi_bridge_table(env, tenant_schema, product, bridge_col, row) %}

{% endmacro %}