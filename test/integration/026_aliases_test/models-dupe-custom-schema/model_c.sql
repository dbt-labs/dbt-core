
-- no custom schema for this model
{{ config(alias='duped_alias') }}

select {{ string_literal(this.name) }} as tablename
