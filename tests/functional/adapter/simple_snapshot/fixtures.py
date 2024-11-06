create_seed_sql = """
create table {schema}.seed (
    id INTEGER,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50),
    gender VARCHAR(50),
    ip_address VARCHAR(20),
    updated_at TIMESTAMP
);
"""

create_snapshot_expected_sql = """
create table {schema}.snapshot_expected (
    id INTEGER,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50),
    gender VARCHAR(50),
    ip_address VARCHAR(20),

    -- snapshotting fields
    updated_at TIMESTAMP,
    test_valid_from TIMESTAMP,
    test_valid_to   TIMESTAMP,
    test_scd_id     TEXT,
    test_updated_at TIMESTAMP
);
"""


seed_insert_sql = """
-- seed inserts
--  use the same email for two users to verify that duplicated check_cols values
--  are handled appropriately
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
(1, 'Judith', 'Kennedy', '(not provided)', 'Female', '54.60.24.128', '2015-12-24 12:19:28'),
(2, 'Arthur', 'Kelly', '(not provided)', 'Male', '62.56.24.215', '2015-10-28 16:22:15'),
(3, 'Rachel', 'Moreno', 'rmoreno2@msu.edu', 'Female', '31.222.249.23', '2016-04-05 02:05:30'),
(4, 'Ralph', 'Turner', 'rturner3@hp.com', 'Male', '157.83.76.114', '2016-08-08 00:06:51'),
(5, 'Laura', 'Gonzales', 'lgonzales4@howstuffworks.com', 'Female', '30.54.105.168', '2016-09-01 08:25:38'),
(6, 'Katherine', 'Lopez', 'klopez5@yahoo.co.jp', 'Female', '169.138.46.89', '2016-08-30 18:52:11'),
(7, 'Jeremy', 'Hamilton', 'jhamilton6@mozilla.org', 'Male', '231.189.13.133', '2016-07-17 02:09:46'),
(8, 'Heather', 'Rose', 'hrose7@goodreads.com', 'Female', '87.165.201.65', '2015-12-29 22:03:56'),
(9, 'Gregory', 'Kelly', 'gkelly8@trellian.com', 'Male', '154.209.99.7', '2016-03-24 21:18:16'),
(10, 'Rachel', 'Lopez', 'rlopez9@themeforest.net', 'Female', '237.165.82.71', '2016-08-20 15:44:49'),
(11, 'Donna', 'Welch', 'dwelcha@shutterfly.com', 'Female', '103.33.110.138', '2016-02-27 01:41:48'),
(12, 'Russell', 'Lawrence', 'rlawrenceb@qq.com', 'Male', '189.115.73.4', '2016-06-11 03:07:09'),
(13, 'Michelle', 'Montgomery', 'mmontgomeryc@scientificamerican.com', 'Female', '243.220.95.82', '2016-06-18 16:27:19'),
(14, 'Walter', 'Castillo', 'wcastillod@pagesperso-orange.fr', 'Male', '71.159.238.196', '2016-10-06 01:55:44'),
(15, 'Robin', 'Mills', 'rmillse@vkontakte.ru', 'Female', '172.190.5.50', '2016-10-31 11:41:21'),
(16, 'Raymond', 'Holmes', 'rholmesf@usgs.gov', 'Male', '148.153.166.95', '2016-10-03 08:16:38'),
(17, 'Gary', 'Bishop', 'gbishopg@plala.or.jp', 'Male', '161.108.182.13', '2016-08-29 19:35:20'),
(18, 'Anna', 'Riley', 'arileyh@nasa.gov', 'Female', '253.31.108.22', '2015-12-11 04:34:27'),
(19, 'Sarah', 'Knight', 'sknighti@foxnews.com', 'Female', '222.220.3.177', '2016-09-26 00:49:06'),
(20, 'Phyllis', 'Fox', null, 'Female', '163.191.232.95', '2016-08-21 10:35:19');
"""


populate_snapshot_expected_sql = """
-- populate snapshot table
insert into {schema}.snapshot_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    test_valid_from,
    test_valid_to,
    test_updated_at,
    test_scd_id
)

select
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    -- fields added by snapshotting
    updated_at as test_valid_from,
    null::timestamp as test_valid_to,
    updated_at as test_updated_at,
    md5(id || '-' || first_name || '|' || updated_at::text) as test_scd_id
from {schema}.seed;
"""

populate_snapshot_expected_valid_to_current_sql = """
-- populate snapshot table
insert into {schema}.snapshot_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    test_valid_from,
    test_valid_to,
    test_updated_at,
    test_scd_id
)

select
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    -- fields added by snapshotting
    updated_at as test_valid_from,
    date('2099-12-31') as test_valid_to,
    updated_at as test_updated_at,
    md5(id || '-' || first_name || '|' || updated_at::text) as test_scd_id
from {schema}.seed;
"""

snapshot_actual_sql = """
{% snapshot snapshot_actual %}

    {{
        config(
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
        )
    }}

    select * from {{target.database}}.{{target.schema}}.seed

{% endsnapshot %}
"""

snapshots_yml = """
snapshots:
  - name: snapshot_actual
    config:
      strategy: timestamp
      updated_at: updated_at
      snapshot_meta_column_names:
          dbt_valid_to: test_valid_to
          dbt_valid_from: test_valid_from
          dbt_scd_id: test_scd_id
          dbt_updated_at: test_updated_at
"""

snapshots_no_column_names_yml = """
snapshots:
  - name: snapshot_actual
    config:
      strategy: timestamp
      updated_at: updated_at
"""

ref_snapshot_sql = """
select * from {{ ref('snapshot_actual') }}
"""


invalidate_sql = """
-- update records 11 - 21. Change email and updated_at field
update {schema}.seed set
    updated_at = updated_at + interval '1 hour',
    email      =  case when id = 20 then 'pfoxj@creativecommons.org' else 'new_' || email end
where id >= 10 and id <= 20;


-- invalidate records 11 - 21
update {schema}.snapshot_expected set
    test_valid_to   = updated_at + interval '1 hour'
where id >= 10 and id <= 20;

"""

update_sql = """
-- insert v2 of the 11 - 21 records

insert into {schema}.snapshot_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    test_valid_from,
    test_valid_to,
    test_updated_at,
    test_scd_id
)

select
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    -- fields added by snapshotting
    updated_at as test_valid_from,
    null::timestamp as test_valid_to,
    updated_at as test_updated_at,
    md5(id || '-' || first_name || '|' || updated_at::text) as test_scd_id
from {schema}.seed
where id >= 10 and id <= 20;
"""

# valid_to_current fixtures

snapshots_valid_to_current_yml = """
snapshots:
  - name: snapshot_actual
    config:
      strategy: timestamp
      updated_at: updated_at
      dbt_valid_to_current: "date('2099-12-31')"
      snapshot_meta_column_names:
          dbt_valid_to: test_valid_to
          dbt_valid_from: test_valid_from
          dbt_scd_id: test_scd_id
          dbt_updated_at: test_updated_at
"""

update_with_current_sql = """
-- insert v2 of the 11 - 21 records

insert into {schema}.snapshot_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    test_valid_from,
    test_valid_to,
    test_updated_at,
    test_scd_id
)

select
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    -- fields added by snapshotting
    updated_at as test_valid_from,
    date('2099-12-31') as test_valid_to,
    updated_at as test_updated_at,
    md5(id || '-' || first_name || '|' || updated_at::text) as test_scd_id
from {schema}.seed
where id >= 10 and id <= 20;
"""


# multi-key snapshot fixtures

create_multi_key_seed_sql = """
create table {schema}.seed (
    id1 INTEGER,
    id2 INTEGER,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50),
    gender VARCHAR(50),
    ip_address VARCHAR(20),
    updated_at TIMESTAMP
);
"""


create_multi_key_snapshot_expected_sql = """
create table {schema}.snapshot_expected (
    id1 INTEGER,
    id2 INTEGER,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50),
    gender VARCHAR(50),
    ip_address VARCHAR(20),

    -- snapshotting fields
    updated_at TIMESTAMP,
    test_valid_from TIMESTAMP,
    test_valid_to   TIMESTAMP,
    test_scd_id     TEXT,
    test_updated_at TIMESTAMP
);
"""

seed_multi_key_insert_sql = """
-- seed inserts
--  use the same email for two users to verify that duplicated check_cols values
--  are handled appropriately
insert into {schema}.seed (id1, id2, first_name, last_name, email, gender, ip_address, updated_at) values
(1, 100,  'Judith', 'Kennedy', '(not provided)', 'Female', '54.60.24.128', '2015-12-24 12:19:28'),
(2, 200, 'Arthur', 'Kelly', '(not provided)', 'Male', '62.56.24.215', '2015-10-28 16:22:15'),
(3, 300, 'Rachel', 'Moreno', 'rmoreno2@msu.edu', 'Female', '31.222.249.23', '2016-04-05 02:05:30'),
(4, 400, 'Ralph', 'Turner', 'rturner3@hp.com', 'Male', '157.83.76.114', '2016-08-08 00:06:51'),
(5, 500, 'Laura', 'Gonzales', 'lgonzales4@howstuffworks.com', 'Female', '30.54.105.168', '2016-09-01 08:25:38'),
(6, 600, 'Katherine', 'Lopez', 'klopez5@yahoo.co.jp', 'Female', '169.138.46.89', '2016-08-30 18:52:11'),
(7, 700, 'Jeremy', 'Hamilton', 'jhamilton6@mozilla.org', 'Male', '231.189.13.133', '2016-07-17 02:09:46'),
(8, 800, 'Heather', 'Rose', 'hrose7@goodreads.com', 'Female', '87.165.201.65', '2015-12-29 22:03:56'),
(9, 900, 'Gregory', 'Kelly', 'gkelly8@trellian.com', 'Male', '154.209.99.7', '2016-03-24 21:18:16'),
(10, 1000, 'Rachel', 'Lopez', 'rlopez9@themeforest.net', 'Female', '237.165.82.71', '2016-08-20 15:44:49'),
(11, 1100, 'Donna', 'Welch', 'dwelcha@shutterfly.com', 'Female', '103.33.110.138', '2016-02-27 01:41:48'),
(12, 1200, 'Russell', 'Lawrence', 'rlawrenceb@qq.com', 'Male', '189.115.73.4', '2016-06-11 03:07:09'),
(13, 1300, 'Michelle', 'Montgomery', 'mmontgomeryc@scientificamerican.com', 'Female', '243.220.95.82', '2016-06-18 16:27:19'),
(14, 1400, 'Walter', 'Castillo', 'wcastillod@pagesperso-orange.fr', 'Male', '71.159.238.196', '2016-10-06 01:55:44'),
(15, 1500, 'Robin', 'Mills', 'rmillse@vkontakte.ru', 'Female', '172.190.5.50', '2016-10-31 11:41:21'),
(16, 1600, 'Raymond', 'Holmes', 'rholmesf@usgs.gov', 'Male', '148.153.166.95', '2016-10-03 08:16:38'),
(17, 1700, 'Gary', 'Bishop', 'gbishopg@plala.or.jp', 'Male', '161.108.182.13', '2016-08-29 19:35:20'),
(18, 1800, 'Anna', 'Riley', 'arileyh@nasa.gov', 'Female', '253.31.108.22', '2015-12-11 04:34:27'),
(19, 1900, 'Sarah', 'Knight', 'sknighti@foxnews.com', 'Female', '222.220.3.177', '2016-09-26 00:49:06'),
(20, 2000, 'Phyllis', 'Fox', null, 'Female', '163.191.232.95', '2016-08-21 10:35:19');
"""

populate_multi_key_snapshot_expected_sql = """
-- populate snapshot table
insert into {schema}.snapshot_expected (
    id1,
    id2,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    test_valid_from,
    test_valid_to,
    test_updated_at,
    test_scd_id
)

select
    id1,
    id2,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    -- fields added by snapshotting
    updated_at as test_valid_from,
    null::timestamp as test_valid_to,
    updated_at as test_updated_at,
    md5(id1::text || '|' || id2::text || '|' || updated_at::text) as test_scd_id
from {schema}.seed;
"""

model_seed_sql = """
select * from {{target.database}}.{{target.schema}}.seed
"""

snapshots_multi_key_yml = """
snapshots:
  - name: snapshot_actual
    relation: "ref('seed')"
    config:
      strategy: timestamp
      updated_at: updated_at
      unique_key:
        - id1
        - id2
      snapshot_meta_column_names:
          dbt_valid_to: test_valid_to
          dbt_valid_from: test_valid_from
          dbt_scd_id: test_scd_id
          dbt_updated_at: test_updated_at
"""

invalidate_multi_key_sql = """
-- update records 11 - 21. Change email and updated_at field
update {schema}.seed set
    updated_at = updated_at + interval '1 hour',
    email      =  case when id1 = 20 then 'pfoxj@creativecommons.org' else 'new_' || email end
where id1 >= 10 and id1 <= 20;


-- invalidate records 11 - 21
update {schema}.snapshot_expected set
    test_valid_to   = updated_at + interval '1 hour'
where id1 >= 10 and id1 <= 20;

"""

update_multi_key_sql = """
-- insert v2 of the 11 - 21 records

insert into {schema}.snapshot_expected (
    id1,
    id2,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    test_valid_from,
    test_valid_to,
    test_updated_at,
    test_scd_id
)

select
    id1,
    id2,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    -- fields added by snapshotting
    updated_at as test_valid_from,
    null::timestamp as test_valid_to,
    updated_at as test_updated_at,
    md5(id1::text || '|' || id2::text || '|' || updated_at::text) as test_scd_id
from {schema}.seed
where id1 >= 10 and id1 <= 20;
"""
