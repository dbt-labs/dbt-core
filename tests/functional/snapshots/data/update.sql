-- insert v2 of the 11 - 21 records

insert into {database}.{schema}.snapshot_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,
    dbt_scd_id
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
    updated_at as dbt_valid_from,
    null::timestamp as dbt_valid_to,
    updated_at as dbt_updated_at,
    md5(id || '-' || first_name || '|' || updated_at::text) as dbt_scd_id
from {database}.{schema}.seed
where id >= 10 and id <= 20;


insert into {database}.{schema}.snapshot_castillo_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    "1-updated_at",
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,
    dbt_scd_id
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
    updated_at as dbt_valid_from,
    null::timestamp as dbt_valid_to,
    updated_at as dbt_updated_at,
    md5(id || '-' || first_name || '|' || updated_at::text) as dbt_scd_id
from {database}.{schema}.seed
where id >= 10 and id <= 20 and last_name = 'Castillo';


insert into {database}.{schema}.snapshot_alvarez_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,
    dbt_scd_id
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
    updated_at as dbt_valid_from,
    null::timestamp as dbt_valid_to,
    updated_at as dbt_updated_at,
    md5(id || '-' || first_name || '|' || updated_at::text) as dbt_scd_id
from {database}.{schema}.seed
where id >= 10 and id <= 20 and last_name = 'Alvarez';


insert into {database}.{schema}.snapshot_kelly_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,
    dbt_scd_id
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
    updated_at as dbt_valid_from,
    null::timestamp as dbt_valid_to,
    updated_at as dbt_updated_at,
    md5(id || '-' || first_name || '|' || updated_at::text) as dbt_scd_id
from {database}.{schema}.seed
where id >= 10 and id <= 20 and last_name = 'Kelly';

-- insert 10 new records
insert into {database}.{schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
(21, 'Judy', 'Robinson', 'jrobinsonk@blogs.com', 'Female', '208.21.192.232', '2016-09-18 08:27:38'),
(22, 'Kevin', 'Alvarez', 'kalvarezl@buzzfeed.com', 'Male', '228.106.146.9', '2016-07-29 03:07:37'),
(23, 'Barbara', 'Carr', 'bcarrm@pen.io', 'Female', '106.165.140.17', '2015-09-24 13:27:23'),
(24, 'William', 'Watkins', 'wwatkinsn@guardian.co.uk', 'Male', '78.155.84.6', '2016-03-08 19:13:08'),
(25, 'Judy', 'Cooper', 'jcoopero@google.com.au', 'Female', '24.149.123.184', '2016-10-05 20:49:33'),
(26, 'Shirley', 'Castillo', 'scastillop@samsung.com', 'Female', '129.252.181.12', '2016-06-20 21:12:21'),
(27, 'Justin', 'Harper', 'jharperq@opera.com', 'Male', '131.172.103.218', '2016-05-21 22:56:46'),
(28, 'Marie', 'Medina', 'mmedinar@nhs.uk', 'Female', '188.119.125.67', '2015-10-08 13:44:33'),
(29, 'Kelly', 'Edwards', 'kedwardss@phoca.cz', 'Female', '47.121.157.66', '2015-09-15 06:33:37'),
(30, 'Carl', 'Coleman', 'ccolemant@wikipedia.org', 'Male', '82.227.154.83', '2016-05-26 16:46:40');


-- add these new records to the snapshot table
insert into {database}.{schema}.snapshot_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,
    dbt_scd_id
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
    updated_at as dbt_valid_from,
    null::timestamp as dbt_valid_to,
    updated_at as dbt_updated_at,
    md5(id || '-' || first_name || '|' || updated_at::text) as dbt_scd_id
from {database}.{schema}.seed
where id > 20;


-- add these new records to the snapshot table
insert into {database}.{schema}.snapshot_castillo_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    "1-updated_at",
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,
    dbt_scd_id
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
    updated_at as dbt_valid_from,
    null::timestamp as dbt_valid_to,
    updated_at as dbt_updated_at,
    md5(id || '-' || first_name || '|' || updated_at::text) as dbt_scd_id
from {database}.{schema}.seed
where id > 20 and last_name = 'Castillo';

insert into {database}.{schema}.snapshot_alvarez_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,
    dbt_scd_id
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
    updated_at as dbt_valid_from,
    null::timestamp as dbt_valid_to,
    updated_at as dbt_updated_at,
    md5(id || '-' || first_name || '|' || updated_at::text) as dbt_scd_id
from {database}.{schema}.seed
where id > 20 and last_name = 'Alvarez';

insert into {database}.{schema}.snapshot_kelly_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,
    dbt_scd_id
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
    updated_at as dbt_valid_from,
    null::timestamp as dbt_valid_to,
    updated_at as dbt_updated_at,
    md5(id || '-' || first_name || '|' || updated_at::text) as dbt_scd_id
from {database}.{schema}.seed
where id > 20 and last_name = 'Kelly';
