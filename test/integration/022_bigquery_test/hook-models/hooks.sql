
{{
  config({
    "pre-hook": "insert into {{this.schema}}.base_table (id, dupe) values (5, 'a')",
    "post-hook": "insert into {{this.schema}}.base_table (id, dupe) values (6, 'a')",
  })
}}

select * from {{ ref('base_table') }}
