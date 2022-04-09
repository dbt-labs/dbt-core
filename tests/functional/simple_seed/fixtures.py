#
# Seeds
#

seeds__enabled_in_config = """seed_id,first_name,email,ip_address,birthday
1,Larry,lking0@miitbeian.gov.cn,69.135.206.194,2008-09-12 19:08:31
2,Larry,lperkins1@toplist.cz,64.210.133.162,1978-05-09 04:15:14
3,Anna,amontgomery2@miitbeian.gov.cn,168.104.64.114,2011-10-16 04:07:57
4,Sandra,sgeorge3@livejournal.com,229.235.252.98,1973-07-19 10:52:43
5,Fred,fwoods4@google.cn,78.229.170.124,2012-09-30 16:38:29
6,Stephen,shanson5@livejournal.com,182.227.157.105,1995-11-07 21:40:50
7,William,wmartinez6@upenn.edu,135.139.249.50,1982-09-05 03:11:59
8,Jessica,jlong7@hao123.com,203.62.178.210,1991-10-16 11:03:15
9,Douglas,dwhite8@tamu.edu,178.187.247.1,1979-10-01 09:49:48
10,Lisa,lcoleman9@nydailynews.com,168.234.128.249,2011-05-26 07:45:49
11,Ralph,rfieldsa@home.pl,55.152.163.149,1972-11-18 19:06:11
12,Louise,lnicholsb@samsung.com,141.116.153.154,2014-11-25 20:56:14
13,Clarence,cduncanc@sfgate.com,81.171.31.133,2011-11-17 07:02:36
14,Daniel,dfranklind@omniture.com,8.204.211.37,1980-09-13 00:09:04
15,Katherine,klanee@auda.org.au,176.96.134.59,1997-08-22 19:36:56
16,Billy,bwardf@wikia.com,214.108.78.85,2003-10-19 02:14:47
17,Annie,agarzag@ocn.ne.jp,190.108.42.70,1988-10-28 15:12:35
18,Shirley,scolemanh@fastcompany.com,109.251.164.84,1988-08-24 10:50:57
19,Roger,rfrazieri@scribd.com,38.145.218.108,1985-12-31 15:17:15
20,Lillian,lstanleyj@goodreads.com,47.57.236.17,1970-06-08 02:09:05

"""

seeds__disabled_in_config = """seed_id,first_name,email,ip_address,birthday
1,Larry,lking0@miitbeian.gov.cn,69.135.206.194,2008-09-12 19:08:31
2,Larry,lperkins1@toplist.cz,64.210.133.162,1978-05-09 04:15:14
3,Anna,amontgomery2@miitbeian.gov.cn,168.104.64.114,2011-10-16 04:07:57
4,Sandra,sgeorge3@livejournal.com,229.235.252.98,1973-07-19 10:52:43
5,Fred,fwoods4@google.cn,78.229.170.124,2012-09-30 16:38:29
6,Stephen,shanson5@livejournal.com,182.227.157.105,1995-11-07 21:40:50
7,William,wmartinez6@upenn.edu,135.139.249.50,1982-09-05 03:11:59
8,Jessica,jlong7@hao123.com,203.62.178.210,1991-10-16 11:03:15
9,Douglas,dwhite8@tamu.edu,178.187.247.1,1979-10-01 09:49:48
10,Lisa,lcoleman9@nydailynews.com,168.234.128.249,2011-05-26 07:45:49
11,Ralph,rfieldsa@home.pl,55.152.163.149,1972-11-18 19:06:11
12,Louise,lnicholsb@samsung.com,141.116.153.154,2014-11-25 20:56:14
13,Clarence,cduncanc@sfgate.com,81.171.31.133,2011-11-17 07:02:36
14,Daniel,dfranklind@omniture.com,8.204.211.37,1980-09-13 00:09:04
15,Katherine,klanee@auda.org.au,176.96.134.59,1997-08-22 19:36:56
16,Billy,bwardf@wikia.com,214.108.78.85,2003-10-19 02:14:47
17,Annie,agarzag@ocn.ne.jp,190.108.42.70,1988-10-28 15:12:35
18,Shirley,scolemanh@fastcompany.com,109.251.164.84,1988-08-24 10:50:57
19,Roger,rfrazieri@scribd.com,38.145.218.108,1985-12-31 15:17:15
20,Lillian,lstanleyj@goodreads.com,47.57.236.17,1970-06-08 02:09:05

"""

# used to tease out include/exclude edge case behavior for 'dbt seed'
seeds__tricky = """\
seed_id,seed_id_str,a_bool,looks_like_a_bool,a_date,looks_like_a_date,relative,weekday
1,1,true,true,2019-01-01 12:32:30,2019-01-01 12:32:30,tomorrow,Saturday
2,2,True,True,2019-01-01 12:32:31,2019-01-01 12:32:31,today,Sunday
3,3,TRUE,TRUE,2019-01-01 12:32:32,2019-01-01 12:32:32,yesterday,Monday
4,4,false,false,2019-01-01 01:32:32,2019-01-01 01:32:32,tomorrow,Saturday
5,5,False,False,2019-01-01 01:32:32,2019-01-01 01:32:32,today,Sunday
6,6,FALSE,FALSE,2019-01-01 01:32:32,2019-01-01 01:32:32,yesterday,Monday

"""


seeds__wont_parse = """a,b,c
1,7,23,90,5
2

"""

#
# Macros
#

macros__schema_test = """
{% test column_type(model, column_name, type) %}

    {% set cols = adapter.get_columns_in_relation(model) %}

    {% set col_types = {} %}
    {% for col in cols %}
        {% do col_types.update({col.name: col.data_type}) %}
    {% endfor %}

    {% set validation_message = 'Got a column type of ' ~ col_types.get(column_name) ~ ', expected ' ~ type %}

    {% set val = 0 if col_types.get(column_name) == type else 1 %}
    {% if val == 1 and execute %}
        {{ log(validation_message, info=True) }}
    {% endif %}

    select '{{ validation_message }}' as validation_error
    from (select true) as nothing
    where {{ val }} = 1

{% endtest %}

"""

#
# Models
#

models__downstream_from_seed_actual = """
select * from {{ ref('seed_actual') }}

"""
models__from_basic_seed = """
select * from {{ this.schema }}.seed_expected

"""

#
# Properties
#

properties__schema_yml = """
version: 2
seeds:
- name: seed_enabled
  columns:
  - name: birthday
    tests:
    - column_type:
        type: date
  - name: seed_id
    tests:
    - column_type:
        type: text

- name: seed_tricky
  columns:
  - name: seed_id
    tests:
    - column_type:
        type: integer
  - name: seed_id_str
    tests:
    - column_type:
        type: text
  - name: a_bool
    tests:
    - column_type:
        type: boolean
  - name: looks_like_a_bool
    tests:
    - column_type:
        type: text
  - name: a_date
    tests:
    - column_type:
        type: timestamp without time zone
  - name: looks_like_a_date
    tests:
    - column_type:
        type: text
  - name: relative
    tests:
    - column_type:
        type: text
  - name: weekday
    tests:
    - column_type:
        type: text

"""
