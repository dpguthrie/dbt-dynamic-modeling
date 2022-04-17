with data as (
    select parse_json(raw) as json
    from {{ source('awards', 'awards_json') }}
),

parsed as (
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
)

select *
from parsed
qualify row_number() over (
    partition by sqlized_tenant_id, sqlized_id
    order by max_timestamp desc
) = 1

