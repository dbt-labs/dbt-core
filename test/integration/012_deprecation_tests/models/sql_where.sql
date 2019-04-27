{{ config(sql_where='id > (select max(id) from {{this}})')}}

select 1 as id
