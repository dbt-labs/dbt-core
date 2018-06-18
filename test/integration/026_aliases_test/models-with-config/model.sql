
-- we use a macro to customize the alias name
-- it should be adds_suffix_ALIAS
{{ config(alias='adds_suffix') }}

select '{{ this.name }}' as tablename
