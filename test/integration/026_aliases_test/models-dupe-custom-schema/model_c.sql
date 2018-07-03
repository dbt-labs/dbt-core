
-- no custom schema for this model
{{ config(alias='duped_alias') }}

select '{{ this.name }}' as tablename
