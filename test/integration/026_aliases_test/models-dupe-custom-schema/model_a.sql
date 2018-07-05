
{{ config(alias='duped_alias', schema='schema_a') }}

select {{ string_literal(this.name) }} as tablename
