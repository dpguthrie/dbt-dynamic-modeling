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
            from flattened
            /*
            This will ensure that we pull in the most recently updated
            column if, for instance, the name of the column (comment) has
            changed.  However, if we have already created this column in
            our inter_fact_<product> table, the current process would NOT
            update that comment to the new column name.
            */
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
            where table_name = 'INTER_FACT_{{ product | upper }}'
        )

        select
            fe.*,
            info.column_name is not null as has_column
        from unique_form_elements fe
        left join info_schema info on
            upper(fe.tenant_schema) = info.table_schema
            and upper(fe.sqlized_id) = info.column_name
    {% endset %}
    
    {% set results = run_query(sql) %}
    {{ return(results) }}

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
