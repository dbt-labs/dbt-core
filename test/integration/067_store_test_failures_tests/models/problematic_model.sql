select * from {{ ref('people') }}

union all

select * from {{ ref('people') }}
where id in (1,2)

union all

select null as id, first_name, last_name, email, gender, ip_address from {{ ref('people') }}
where id in (3,4)
