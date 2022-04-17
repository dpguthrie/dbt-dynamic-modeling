{% macro sqlize_column() %}

drop function if exists {{ target.schema }}.sqlize_column(varchar);

create or replace function sqlize_column("col" varchar)
    returns varchar
    language javascript
    as $$
    
        var reservedWords = ['ACCOUNT', 'ALL', 'ALTER', 'AND', 'ANY', 'AS', 'BETWEEN', 'BY', 'CASE', 'CAST', 'CHECK', 'COLUMN',
               'CONNECT', 'CONNECTION', 'CONSTRAINT', 'CREATE', 'CROSS', 'CURRENT', 'CURRENT_DATE', 'CURRENT_TIME',
               'CURRENT_TIMESTAMP', 'CURRENT_USER', 'DATABASE', 'DELETE', 'DISTINCT', 'DROP', 'ELSE', 'EXISTS', 'FALSE',
               'FOLLOWING', 'FOR', 'FROM', 'FULL', 'GRANT', 'GROUP', 'GSCLUSTER', 'HAVING', 'ILIKE', 'IN', 'INCREMENT',
               'INNER', 'INSERT', 'INTERSECT', 'INTO', 'IS', 'ISSUE', 'JOIN', 'LATERAL', 'LEFT', 'LIKE', 'LOCALTIME',
               'LOCALTIMESTAMP', 'MINUS', 'NATURAL', 'NOT', 'NULL', 'OF', 'ON', 'OR', 'ORDER', 'ORGANIZATION',
               'QUALIFY', 'REGEXP', 'REVOKE', 'RIGHT', 'RLIKE', 'ROW', 'ROWS', 'SAMPLE', 'SCHEMA', 'SELECT', 'SET',
               'SOME', 'START', 'TABLE', 'TABLESAMPLE', 'THEN', 'TO', 'TRIGGER', 'TRUE', 'TRY_CAST', 'UNION', 'UNIQUE',
               'UPDATE', 'USING', 'VALUES', 'VIEW', 'WHEN', 'WHENEVER', 'WHERE', 'WITH'];
        
        var col = col.replaceAll(/[\W_]+/g, "_");
        
        if (!isNaN(col.charAt(0)) || reservedWords.includes(col.toUpperCase())) {
            col = '_' + col;
        }
        col = col.substring(0, 250);
        return col;
    
    $$;

{% endmacro %}