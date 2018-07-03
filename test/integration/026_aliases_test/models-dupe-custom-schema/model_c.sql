
-- no custom schema for this model
{{ config(alias='duped_alias') }}

select
    regexp_replace('{{ this.schema }}', 'test[0-9]+_', '') as schemaname,
    '{{ this.name }}' as tablename
