{% macro sqlize_tenant_id() %}

drop function if exists {{ target.schema }}.sqlize_tenant_id(varchar);

create or replace function sqlize_tenant_id("tenant_id" varchar)
    returns varchar
    language javascript
    as $$
    
        var tenant_id = tenant_id.replaceAll('-', '_');
        tenant_id = tenant_id.substring(0, 254);
        return tenant_id;
    
    $$;

{% endmacro %}
