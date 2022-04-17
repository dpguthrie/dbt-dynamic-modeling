

{% macro get_current_form_elements(env, product) %}

    {% set sql %}
        with data as (
            select parse_json(raw) as json
            from {{ env }}.products.{{ product }}
        ),

        flattened as (
            select
                data.json:tenantId::string as tenant_id,
                sqlize_tenant_id(data.json:tenantId::string) as sqlized_tenant_id,
                a.value:type::string as data_type,
                a.value:cayuseType::string as cayuse_type,
                a.value:columnHeader::string as comment,
                a.value:id::string as id,
                sqlize_column(a.value:id::string) as sqlized_id,
                coalesce(get(a.value:extraSettings, 'multi')::boolean, false) as multiple_values,
                max(data.json:updateDate::timestamp_ntz) as max_timestamp
            from data,
                table(flatten(json:formElements)) a
            group by 1, 2, 3, 4, 5, 6, 7, 8
        ),

        unique_form_elements as (
            select 
                *,
                'analytics_' || sqlized_tenant_id as tenant_schema
            from parsed
            qualify row_number() over (
                partition by sqlized_tenant_id, sqlized_id
                order by max_timestamp desc
            ) = 1
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
        from unique_form_elements fe
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

    {% set sql %}
    
        select schema_name
        from {{ env }}.information_schema.schemata
        where schema_name like 'ANALYTICS_%'

    {% endset %}

    {% set schemas = run_query(sql).columns[0].values() %}

    {{ return(schemas) }}

{% endmacro %}

{% macro get_current_tenants(env, product) %}

    {% set sql %}

    with data as (
        select parse_json(raw) as json
        from {{ env }}.products.{{ product }}
    )

    select distinct
        data.json:tenantId::string as tenant_id,
        sqlize_tenant_id(data.json:tenantId::string) as sqlized_tenant_id,
        upper('analytics_' || sqlized_tenant_id) as tenant_schema
    from data

    {% endset %}

    {% if execute %}

        {% set results = run_query(sql) %}
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
