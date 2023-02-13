#
# Seeds
#
seeds__people = """id,first_name,last_name,email,gender,ip_address
1,Jack,Hunter,jhunter0@pbs.org,Male,59.80.20.168
2,Kathryn,Walker,kwalker1@ezinearticles.com,Female,194.121.179.35
3,Gerald,Ryan,gryan2@com.com,Male,11.3.212.243
4,Bonnie,Spencer,bspencer3@ameblo.jp,Female,216.32.196.175
5,Harold,Taylor,htaylor4@people.com.cn,Male,253.10.246.136
6,Jacqueline,Griffin,jgriffin5@t.co,Female,16.13.192.220
7,Wanda,Arnold,warnold6@google.nl,Female,232.116.150.64
8,Craig,Ortiz,cortiz7@sciencedaily.com,Male,199.126.106.13
9,Gary,Day,gday8@nih.gov,Male,35.81.68.186
10,Rose,Wright,rwright9@yahoo.co.jp,Female,236.82.178.100
"""

seeds__expected_accepted_values = """value_field,n_records
Gary,1
Rose,1
"""

seeds__expected_failing_test = """id,first_name,last_name,email,gender,ip_address
1,Jack,Hunter,jhunter0@pbs.org,Male,59.80.20.168
2,Kathryn,Walker,kwalker1@ezinearticles.com,Female,194.121.179.35
3,Gerald,Ryan,gryan2@com.com,Male,11.3.212.243
4,Bonnie,Spencer,bspencer3@ameblo.jp,Female,216.32.196.175
5,Harold,Taylor,htaylor4@people.com.cn,Male,253.10.246.136
6,Jacqueline,Griffin,jgriffin5@t.co,Female,16.13.192.220
7,Wanda,Arnold,warnold6@google.nl,Female,232.116.150.64
8,Craig,Ortiz,cortiz7@sciencedaily.com,Male,199.126.106.13
9,Gary,Day,gday8@nih.gov,Male,35.81.68.186
10,Rose,Wright,rwright9@yahoo.co.jp,Female,236.82.178.100
"""

seeds__expected_not_null_problematic_model_id = """id,first_name,last_name,email,gender,ip_address
,Gerald,Ryan,gryan2@com.com,Male,11.3.212.243
,Bonnie,Spencer,bspencer3@ameblo.jp,Female,216.32.196.175
"""

seeds__expected_unique_problematic_model_id = """unique_field,n_records
2,2
1,2
"""

#
# Schema
#
properties__schema_yml = """
version: 2

models:

  - name: fine_model
    columns:
      - name: id
        tests:
          - unique
          - not_null

  - name: problematic_model
    columns:
      - name: id
        tests:
          - unique:
              store_failures: true
          - not_null
      - name: first_name
        tests:
          # test truncation of really long test name
          - accepted_values:
              values:
                - Jack
                - Kathryn
                - Gerald
                - Bonnie
                - Harold
                - Jacqueline
                - Wanda
                - Craig
                # - Gary
                # - Rose

  - name: fine_model_but_with_a_no_good_very_long_name
    columns:
      - name: quite_long_column_name
        tests:
          # test truncation of really long test name with builtin
          - unique
"""

#
# Models
#
models__fine_model = """
select * from {{ ref('people') }}
"""

models__file_model_but_with_a_no_good_very_long_name = """
select 1 as quite_long_column_name
"""

models__problematic_model = """
select * from {{ ref('people') }}

union all

select * from {{ ref('people') }}
where id in (1,2)

union all

select null as id, first_name, last_name, email, gender, ip_address from {{ ref('people') }}
where id in (3,4)
"""

#
# Tests
#
tests__failing_test = """
select * from {{ ref('fine_model') }}
"""

tests__passing_test = """
select * from {{ ref('fine_model') }}
where false
"""
