error_models__schema_yml = """version: 2
sources:
  - name: test_source
    loader: custom
    freshness:
      warn_after: {count: 10, period: hour}
      error_after: {count: 1, period: day}
    schema: invalid
    tables:
      - name: test_table
        identifier: source
        loaded_at_field: updated_at
"""

error_models__model_sql = """select * from {{ source('test_source', 'test_table') }}
"""

override_freshness_models__schema_yml = """version: 2
sources:
  - name: test_source
    loader: custom
    freshness: # default freshness
      warn_after: {count: 12, period: hour}
      error_after: {count: 24, period: hour}
    schema: "{{ var(env_var('DBT_TEST_SCHEMA_NAME_VARIABLE')) }}"
    loaded_at_field: loaded_at
    quoting:
      identifier: True
    tags:
      - my_test_source_tag
    tables:
      - name: source_a
        identifier: source
        loaded_at_field: "{{ var('test_loaded_at') | as_text }}"
        freshness:
          warn_after: {count: 6, period: hour}
          # use the default error_after defined above
      - name: source_b
        identifier: source
        loaded_at_field: "{{ var('test_loaded_at') | as_text }}"
        freshness:
          warn_after: {count: 6, period: hour}
          error_after: {} # use the default error_after defined above
      - name: source_c
        identifier: source
        loaded_at_field: "{{ var('test_loaded_at') | as_text }}"
        freshness:
          warn_after: {count: 6, period: hour}
          error_after: null # override: disable error_after for this table
      - name: source_d
        identifier: source
        loaded_at_field: "{{ var('test_loaded_at') | as_text }}"
        freshness:
          warn_after: {count: 6, period: hour}
          error_after: {count: 72, period: hour} # override: use this new behavior instead of error_after defined above
      - name: source_e
        identifier: source
        loaded_at_field: "{{ var('test_loaded_at') | as_text }}"
        freshness: null # override: disable freshness for this table
"""

models__schema_yml = """version: 2
models:
  - name: descendant_model
    columns:
      - name: favorite_color
        tests:
          - relationships:
             to: source('test_source', 'test_table')
             field: favorite_color
      - name: id
        tests:
          - unique
          - not_null

sources:
  - name: test_source
    loader: custom
    freshness:
      warn_after: {count: 10, period: hour}
      error_after: {count: 1, period: day}
    schema: "{{ var(env_var('DBT_TEST_SCHEMA_NAME_VARIABLE')) }}"
    quoting:
      identifier: True
    tags:
      - my_test_source_tag
    tables:
      - name: test_table
        identifier: source
        loaded_at_field: "{{ var('test_loaded_at') | as_text }}"
        freshness:
          error_after: {count: 18, period: hour}
        tags:
          - my_test_source_table_tag
        columns:
          - name: favorite_color
            description: The favorite color
          - name: id
            description: The user ID
            tests:
              - unique
              - not_null
            tags:
              - id_column
          - name: first_name
            description: The first name of the user
            tests: []
          - name: email
            description: The email address of the user
          - name: ip_address
            description: The last IP address the user logged in from
          - name: updated_at
            description: The last update time for this user
        tests:
          - relationships:
              # do this as a table-level test, just to test out that aspect
              column_name: favorite_color
              to: ref('descendant_model')
              field: favorite_color
      - name: other_test_table
        identifier: other_table
        columns:
          - name: id
            tests:
              - not_null
              - unique
            tags:
              - id_column
      - name: disabled_test_table
        freshness: null
        loaded_at_field: "{{ var('test_loaded_at') | as_text }}"
  - name: other_source
    schema: "{{ var('test_run_schema') }}"
    quoting:
      identifier: True
    tables:
      - name: test_table
        identifier: other_source_table
  - name: external_source
    schema: "{{ var('test_run_alt_schema', var('test_run_schema')) }}"
    tables:
      - name: table
"""

models__view_model_sql = """{# See here: https://github.com/dbt-labs/dbt-core/pull/1729 #}

select * from {{ ref('ephemeral_model') }}
"""

models__ephemeral_model_sql = """{{ config(materialized='ephemeral') }}

select 1 as id
"""

models__descendant_model_sql = """select * from {{ source('test_source', 'test_table') }}
"""

models__multi_source_model_sql = """select * from {{ source('test_source', 'other_test_table')}}
  join {{ source('other_source', 'test_table')}} using (id)
"""

models__nonsource_descendant_sql = """select * from {{ schema }}.source
"""

models__newly_added_model_sql = """select 2 as id"""

models__newly_added_error_model_sql = """select error from fake_table"""

malformed_models__schema_yml = """version: 2
sources:
  - name: test_source
    loader: custom
    schema: "{{ var('test_run_schema') }}"
    tables:
      - name: test_table
        identifier: source
        tests:
          - relationships:
            # this is invalid (list of 3 1-key dicts instead of a single 3-key dict)
              - column_name: favorite_color
              - to: ref('descendant_model')
              - field: favorite_color
"""

malformed_models__descendant_model_sql = """select * from {{ source('test_source', 'test_table') }}
"""

filtered_models__schema_yml = """version: 2
sources:
  - name: test_source
    loader: custom
    freshness:
      warn_after: {count: 10, period: hour}
      error_after: {count: 1, period: day}
      filter: id > 1
    schema: "{{ var(env_var('DBT_TEST_SCHEMA_NAME_VARIABLE')) }}"
    quoting:
      identifier: True
    tables:
      - name: test_table
        identifier: source
        loaded_at_field: updated_at
        freshness:
          error_after: {count: 18, period: hour}
          filter: id > 101
"""

macros__macro_sql = """{% macro override_me() -%}
    {{ exceptions.raise_compiler_error('this is a bad macro') }}
{%- endmacro %}

{% macro happy_little_macro() -%}
    {{ override_me() }}
{%- endmacro %}


{% macro vacuum_source(source_name, table_name) -%}
    {% call statement('stmt', auto_begin=false, fetch_result=false) %}
        vacuum {{ source(source_name, table_name) }}
    {% endcall %}
{%- endmacro %}
"""

seeds__source_csv = """favorite_color,id,first_name,email,ip_address,updated_at
blue,1,Larry,lking0@miitbeian.gov.cn,'69.135.206.194',2008-09-12 19:08:31
blue,2,Larry,lperkins1@toplist.cz,'64.210.133.162',1978-05-09 04:15:14
blue,3,Anna,amontgomery2@miitbeian.gov.cn,'168.104.64.114',2011-10-16 04:07:57
blue,4,Sandra,sgeorge3@livejournal.com,'229.235.252.98',1973-07-19 10:52:43
blue,5,Fred,fwoods4@google.cn,'78.229.170.124',2012-09-30 16:38:29
blue,6,Stephen,shanson5@livejournal.com,'182.227.157.105',1995-11-07 21:40:50
blue,7,William,wmartinez6@upenn.edu,'135.139.249.50',1982-09-05 03:11:59
blue,8,Jessica,jlong7@hao123.com,'203.62.178.210',1991-10-16 11:03:15
blue,9,Douglas,dwhite8@tamu.edu,'178.187.247.1',1979-10-01 09:49:48
blue,10,Lisa,lcoleman9@nydailynews.com,'168.234.128.249',2011-05-26 07:45:49
blue,11,Ralph,rfieldsa@home.pl,'55.152.163.149',1972-11-18 19:06:11
blue,12,Louise,lnicholsb@samsung.com,'141.116.153.154',2014-11-25 20:56:14
blue,13,Clarence,cduncanc@sfgate.com,'81.171.31.133',2011-11-17 07:02:36
blue,14,Daniel,dfranklind@omniture.com,'8.204.211.37',1980-09-13 00:09:04
blue,15,Katherine,klanee@auda.org.au,'176.96.134.59',1997-08-22 19:36:56
blue,16,Billy,bwardf@wikia.com,'214.108.78.85',2003-10-19 02:14:47
blue,17,Annie,agarzag@ocn.ne.jp,'190.108.42.70',1988-10-28 15:12:35
blue,18,Shirley,scolemanh@fastcompany.com,'109.251.164.84',1988-08-24 10:50:57
blue,19,Roger,rfrazieri@scribd.com,'38.145.218.108',1985-12-31 15:17:15
blue,20,Lillian,lstanleyj@goodreads.com,'47.57.236.17',1970-06-08 02:09:05
blue,21,Aaron,arodriguezk@nps.gov,'205.245.118.221',1985-10-11 23:07:49
blue,22,Patrick,pparkerl@techcrunch.com,'19.8.100.182',2006-03-29 12:53:56
blue,23,Phillip,pmorenom@intel.com,'41.38.254.103',2011-11-07 15:35:43
blue,24,Henry,hgarcian@newsvine.com,'1.191.216.252',2008-08-28 08:30:44
blue,25,Irene,iturnero@opera.com,'50.17.60.190',1994-04-01 07:15:02
blue,26,Andrew,adunnp@pen.io,'123.52.253.176',2000-11-01 06:03:25
blue,27,David,dgutierrezq@wp.com,'238.23.203.42',1988-01-25 07:29:18
blue,28,Henry,hsanchezr@cyberchimps.com,'248.102.2.185',1983-01-01 13:36:37
blue,29,Evelyn,epetersons@gizmodo.com,'32.80.46.119',1979-07-16 17:24:12
blue,30,Tammy,tmitchellt@purevolume.com,'249.246.167.88',2001-04-03 10:00:23
blue,31,Jacqueline,jlittleu@domainmarket.com,'127.181.97.47',1986-02-11 21:35:50
blue,32,Earl,eortizv@opera.com,'166.47.248.240',1996-07-06 08:16:27
blue,33,Juan,jgordonw@sciencedirect.com,'71.77.2.200',1987-01-31 03:46:44
blue,34,Diane,dhowellx@nyu.edu,'140.94.133.12',1994-06-11 02:30:05
blue,35,Randy,rkennedyy@microsoft.com,'73.255.34.196',2005-05-26 20:28:39
blue,36,Janice,jriveraz@time.com,'22.214.227.32',1990-02-09 04:16:52
blue,37,Laura,lperry10@diigo.com,'159.148.145.73',2015-03-17 05:59:25
blue,38,Gary,gray11@statcounter.com,'40.193.124.56',1970-01-27 10:04:51
blue,39,Jesse,jmcdonald12@typepad.com,'31.7.86.103',2009-03-14 08:14:29
blue,40,Sandra,sgonzalez13@goodreads.com,'223.80.168.239',1993-05-21 14:08:54
blue,41,Scott,smoore14@archive.org,'38.238.46.83',1980-08-30 11:16:56
blue,42,Phillip,pevans15@cisco.com,'158.234.59.34',2011-12-15 23:26:31
blue,43,Steven,sriley16@google.ca,'90.247.57.68',2011-10-29 19:03:28
blue,44,Deborah,dbrown17@hexun.com,'179.125.143.240',1995-04-10 14:36:07
blue,45,Lori,lross18@ow.ly,'64.80.162.180',1980-12-27 16:49:15
blue,46,Sean,sjackson19@tumblr.com,'240.116.183.69',1988-06-12 21:24:45
blue,47,Terry,tbarnes1a@163.com,'118.38.213.137',1997-09-22 16:43:19
blue,48,Dorothy,dross1b@ebay.com,'116.81.76.49',2005-02-28 13:33:24
blue,49,Samuel,swashington1c@house.gov,'38.191.253.40',1989-01-19 21:15:48
blue,50,Ralph,rcarter1d@tinyurl.com,'104.84.60.174',2007-08-11 10:21:49
green,51,Wayne,whudson1e@princeton.edu,'90.61.24.102',1983-07-03 16:58:12
green,52,Rose,rjames1f@plala.or.jp,'240.83.81.10',1995-06-08 11:46:23
green,53,Louise,lcox1g@theglobeandmail.com,'105.11.82.145',2016-09-19 14:45:51
green,54,Kenneth,kjohnson1h@independent.co.uk,'139.5.45.94',1976-08-17 11:26:19
green,55,Donna,dbrown1i@amazon.co.uk,'19.45.169.45',2006-05-27 16:51:40
green,56,Johnny,jvasquez1j@trellian.com,'118.202.238.23',1975-11-17 08:42:32
green,57,Patrick,pramirez1k@tamu.edu,'231.25.153.198',1997-08-06 11:51:09
green,58,Helen,hlarson1l@prweb.com,'8.40.21.39',1993-08-04 19:53:40
green,59,Patricia,pspencer1m@gmpg.org,'212.198.40.15',1977-08-03 16:37:27
green,60,Joseph,jspencer1n@marriott.com,'13.15.63.238',2005-07-23 20:22:06
green,61,Phillip,pschmidt1o@blogtalkradio.com,'177.98.201.190',1976-05-19 21:47:44
green,62,Joan,jwebb1p@google.ru,'105.229.170.71',1972-09-07 17:53:47
green,63,Phyllis,pkennedy1q@imgur.com,'35.145.8.244',2000-01-01 22:33:37
green,64,Katherine,khunter1r@smh.com.au,'248.168.205.32',1991-01-09 06:40:24
green,65,Laura,lvasquez1s@wiley.com,'128.129.115.152',1997-10-23 12:04:56
green,66,Juan,jdunn1t@state.gov,'44.228.124.51',2004-11-10 05:07:35
green,67,Judith,jholmes1u@wiley.com,'40.227.179.115',1977-08-02 17:01:45
green,68,Beverly,bbaker1v@wufoo.com,'208.34.84.59',2016-03-06 20:07:23
green,69,Lawrence,lcarr1w@flickr.com,'59.158.212.223',1988-09-13 06:07:21
green,70,Gloria,gwilliams1x@mtv.com,'245.231.88.33',1995-03-18 22:32:46
green,71,Steven,ssims1y@cbslocal.com,'104.50.58.255',2001-08-05 21:26:20
green,72,Betty,bmills1z@arstechnica.com,'103.177.214.220',1981-12-14 21:26:54
green,73,Mildred,mfuller20@prnewswire.com,'151.158.8.130',2000-04-19 10:13:55
green,74,Donald,dday21@icq.com,'9.178.102.255',1972-12-03 00:58:24
green,75,Eric,ethomas22@addtoany.com,'85.2.241.227',1992-11-01 05:59:30
green,76,Joyce,jarmstrong23@sitemeter.com,'169.224.20.36',1985-10-24 06:50:01
green,77,Maria,mmartinez24@amazonaws.com,'143.189.167.135',2005-10-05 05:17:42
green,78,Harry,hburton25@youtube.com,'156.47.176.237',1978-03-26 05:53:33
green,79,Kevin,klawrence26@hao123.com,'79.136.183.83',1994-10-12 04:38:52
green,80,David,dhall27@prweb.com,'133.149.172.153',1976-12-15 16:24:24
green,81,Kathy,kperry28@twitter.com,'229.242.72.228',1979-03-04 02:58:56
green,82,Adam,aprice29@elegantthemes.com,'13.145.21.10',1982-11-07 11:46:59
green,83,Brandon,bgriffin2a@va.gov,'73.249.128.212',2013-10-30 05:30:36
green,84,Henry,hnguyen2b@discovery.com,'211.36.214.242',1985-01-09 06:37:27
green,85,Eric,esanchez2c@edublogs.org,'191.166.188.251',2004-05-01 23:21:42
green,86,Jason,jlee2d@jimdo.com,'193.92.16.182',1973-01-08 09:05:39
green,87,Diana,drichards2e@istockphoto.com,'19.130.175.245',1994-10-05 22:50:49
green,88,Andrea,awelch2f@abc.net.au,'94.155.233.96',2002-04-26 08:41:44
green,89,Louis,lwagner2g@miitbeian.gov.cn,'26.217.34.111',2003-08-25 07:56:39
green,90,Jane,jsims2h@seesaa.net,'43.4.220.135',1987-03-20 20:39:04
green,91,Larry,lgrant2i@si.edu,'97.126.79.34',2000-09-07 20:26:19
green,92,Louis,ldean2j@prnewswire.com,'37.148.40.127',2011-09-16 20:12:14
green,93,Jennifer,jcampbell2k@xing.com,'38.106.254.142',1988-07-15 05:06:49
green,94,Wayne,wcunningham2l@google.com.hk,'223.28.26.187',2009-12-15 06:16:54
green,95,Lori,lstevens2m@icq.com,'181.250.181.58',1984-10-28 03:29:19
green,96,Judy,jsimpson2n@marriott.com,'180.121.239.219',1986-02-07 15:18:10
green,97,Phillip,phoward2o@usa.gov,'255.247.0.175',2002-12-26 08:44:45
green,98,Gloria,gwalker2p@usa.gov,'156.140.7.128',1997-10-04 07:58:58
green,99,Paul,pjohnson2q@umn.edu,'183.59.198.197',1991-11-14 12:33:55
green,100,Frank,fgreene2r@blogspot.com,'150.143.68.121',2010-06-12 23:55:39
"""

seeds__other_table_csv = """id,first_name
1,Larry
2,Curly
3,Moe
"""

seeds__expected_multi_source_csv = """id,first_name,color
1,Larry,blue
2,Curly,red
3,Moe,green
"""

seeds__other_source_table_csv = """id,color
1,blue
2,red
3,green
"""

malformed_schema_tests__schema_yml = """version: 2
sources:
  - name: test_source
    schema: "{{ var('test_run_schema') }}"
    tables:
      - name: test_table
        identifier: source
        columns:
          - name: favorite_color
            tests:
              - relationships:
                  to: ref('model')
                  # this will get rendered as its literal
                  field: "{{ 'favorite' ~ 'color' }}"
"""

malformed_schema_tests__model_sql = """select * from {{ source('test_source', 'test_table') }}
"""
