_TABLE_ONE = """
select * from {{ ref('seed') }}
"""
_TABLE_ONE_DOT_MODEL_SCHEMA = "first_schema"
_TABLE_ONE_DOT_MODEL_NAME = f"{_TABLE_ONE_DOT_MODEL_SCHEMA}.view_1"
_TABLE_ONE_DOT_MODEL = """
select * from {{ target.schema }}.seed
"""

_TABLE_TWO_SCHEMA = "custom"
_TABLE_TWO = (
    """
{{ config(schema='"""
    + _TABLE_TWO_SCHEMA
    + """') }}
select * from {{ ref('view_1') }}
"""
)
_TABLE_TWO_DOT_MODEL_SCHEMA = "second_schema"
_TABLE_TWO_DOT_MODEL_NAME = f"{_TABLE_TWO_DOT_MODEL_SCHEMA}.view_2"
_TABLE_TWO_DOT_MODEL = "select * from {{ ref('" + _TABLE_ONE_DOT_MODEL_NAME + "') }}"

_TABLE_THREE_SCHEMA = "test"
_TABLE_THREE = (
    """
{{ config(materialized='table', schema='"""
    + _TABLE_THREE_SCHEMA
    + """') }}


with v1 as (

    select * from{{ ref('view_1') }}

),

v2 as (

    select * from {{ ref('view_2') }}

),

combined as (

    select last_name from v1
    union all
    select last_name from v2

)

select
    last_name,
    count(*) as count

from combined
group by 1
"""
)

_TABLE_THREE_DOT_MODEL = """
{{ config(materialized='table') }}


with v1 as (

    select * from {{ ref('first_schema.view_1') }}

),

v2 as (

    select * from {{ ref('second_schema.view_2') }}

),

combined as (

    select last_name from v1
    union all
    select last_name from v2

)

select
    last_name,
    count(*) as count

from combined
group by 1
"""

_SEED_CSV = """id,first_name,last_name,email,gender,ip_address
1,Jack,Hunter,jhunter0@pbs.org,Male,59.80.20.168
2,Kathryn,Walker,kwalker1@ezinearticles.com,Female,194.121.179.35
3,Gerald,Ryan,gryan2@com.com,Male,11.3.212.243"""

_CUSTOM_CONFIG = """
{{ config(schema='custom') }}

select * from {{ ref('view_1') }}
"""

_VALIDATION_SQL = """
drop table if exists {database}.{schema}.seed cascade;
create table {database}.{schema}.seed (
   id BIGSERIAL PRIMARY KEY,
   first_name VARCHAR(50),
   last_name VARCHAR(50),
   email VARCHAR(50),
   gender VARCHAR(50),
   ip_address VARCHAR(20)
);

drop table if exists {database}.{schema}.agg cascade;
create table {database}.{schema}.agg (
   last_name VARCHAR(50),
   count BIGINT
);


insert into {database}.{schema}.seed (first_name, last_name, email, gender, ip_address) values
('Jack', 'Hunter', 'jhunter0@pbs.org', 'Male', '59.80.20.168'),
('Kathryn', 'Walker', 'kwalker1@ezinearticles.com', 'Female', '194.121.179.35'),
('Gerald', 'Ryan', 'gryan2@com.com', 'Male', '11.3.212.243');

insert into {database}.{schema}.agg (last_name, count) values
('Hunter', 2), ('Walker', 2), ('Ryan', 2);
"""
