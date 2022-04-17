with raw_data as (
    select parse_json(raw) as json
    from {{ source('awards', 'awards_json') }}
    qualify row_number() over (
        partition by json:awardId
        order by json:modificationNumber desc
    ) = 1
)

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
    json:createdDate as create_date,
    json:updatedDate as update_date,
    json:createdUser::string as create_user,
    json:updatedUser::string as update_user,
    json:statusName::string as status_name,
    json:statusKey::string as status_key,
    json:formElements as form_elements_json,
    sqlize_tenant_id(json:tenantId::string) as sqlized_tenant_id
from raw_data
