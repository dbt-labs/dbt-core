
{{ config(alias='duped_alias', schema='schema_a') }}

select
    regexp_replace('{{ this.schema }}', 'test[0-9]+_', '') as schemaname,
    '{{ this.name }}' as tablename
