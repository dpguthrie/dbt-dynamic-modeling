{% macro array_compare() %}

drop function if exists {{ target.schema }}.array_compare(varchar);

create or replace function array_compare("arr1" array, "arr2" array)
    returns array
    language javascript
    as $$
        
        var missing = arr2.filter(e => !arr1.includes(e));
        return missing;
    
    $$;

{% endmacro %}