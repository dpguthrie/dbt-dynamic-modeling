{% macro get_current_form_elements(env) %}

    {% set sql %}
        with form_elements as (
            select
                *,
                'analytics_' || sqlized_tenant_id as tenant_schema
            from {{ ref('stg_awards__distinct_form_elements') }}
        ),

        info_schema as (
            select
                column_name,
                table_schema
            from {{ env }}.information_schema.columns
            where table_name = 'INTER_FACT_AWARD'
        )

        select
            fe.*,
            info.column_name is not null as has_column
        from form_elements fe
        left join info_schema info on
            upper(fe.tenant_schema) = info.table_schema
            and upper(fe.sqlized_id) = info.column_name
    {% endset %}
    
    {% if execute %}
        {% set results = run_query(sql) %}
        {{ return(results) }}
    {% endif %}

{% endmacro %}

{% macro get_current_schemas(env) %}

    {% set raw_sql %}
    
        select schema_name
        from {{ env }}.information_schema.schemata
        where schema_name like 'ANALYTICS_%'

    {% endset %}

    {% set schemas = run_query(raw_sql).columns[0].values() %}

    {{ return(schemas) }}

{% endmacro %}

{% macro get_current_tenants() %}

    {% set tenants_sql %}

    select * from {{ ref('stg_awards__distinct_tenants') }}

    {% endset %}

    {% if execute %}

        {% set results = run_query(tenants_sql) %}
        {{ return(results) }}

    {% endif %}

{% endmacro %}

{% macro get_missing_columns_from_inter_fact(env, product, sqlized_tenant_id, tenant_schema) %}

    {% set tenant_schema = 'analytics_' + sqlized_tenant_id | upper %}
    {% set table_name = 'inter_fact_' + product | upper %}
    {% set model_name = 'stg_' + product + 's__distinct_form_elements' %}

    {% set sql %}
    with info_schema as (
        select
            column_name,
            data_type
        from {{ env }}.information_schema.columns
        where table_schema = {{ tenant_schema }}
            and table_name = {{ table_name }}
    ),

    existing_columns as (
        select *
        from {{ ref(model_name) }}
        where sqlized_tenant_id = {{ sqlized_tenant_id }}
    )

    select existing_columns.*
    from existing_columns
    left join info_schema on
        existing_columns.sqlized_id = info_schema.column_name
    where info_schema.column_name is null
    {% endset %}

    {% set results = run_query(sql) %}
    {{ return(results) }}

{% endmacro %}
