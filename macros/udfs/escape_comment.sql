{% macro escape_comment() %}

drop function if exists {{ target.schema }}.escape_comment(varchar);

create or replace function escape_comment("comment" varchar)
    returns varchar
    language javascript
    as $$
        
        var comment = comment;
    
        if (comment) {
            comment.replace("'", "\\'");
        } else {
            comment = ''
        }
        
        comment = "'" + comment + "'";
        return comment;
    
    $$;

{% endmacro %}