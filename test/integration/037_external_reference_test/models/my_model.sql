{{
  config(
    materialized = "view"
  )
}}

select * from "{{ this.schema + 'z' }}"."external"
