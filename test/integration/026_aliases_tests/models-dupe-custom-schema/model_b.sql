
{{ config(alias='duped_alias', schema='schema_b') }}

select {{ string_literal(this.name) }} as tablename
