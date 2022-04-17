 {% macro create_udfs() %}

   use database {{ target.database }};

   {{ array_compare() }}   

   {{ escape_comment() }}

   {{ sqlize_column() }}

   {{ sqlize_tenant_id() }}

 {% endmacro %}