with data as (
    select parse_json(raw) as json
    from {{ source('awards', 'awards_json') }}
)

select distinct
    data.json:tenantId::string as tenant_id,
    sqlize_tenant_id(data.json:tenantId::string) as sqlized_tenant_id,
    upper('analytics_' || sqlized_tenant_id) as tenant_schema
from data
