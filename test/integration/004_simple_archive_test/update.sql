
-- update records 11 - 21. Change email and updated_at field
update simple_archive_004.seed set
    updated_at = updated_at + interval '1 hour',
    email      = 'new_' || email
where id >= 10 and id <= 20;


-- invalidate records 11 - 21
update simple_archive_004.archive_expected set
    valid_to   = updated_at + interval '1 hour'
where id >= 10 and id <= 20;

-- insert v2 of the 11 - 21 records

insert into simple_archive_004.archive_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    valid_from,
    valid_to,
    dbt_updated_at,
    scd_id
)

select
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    -- fields added by archival
    updated_at as valid_from,
    null::timestamp as valid_to,
    updated_at as dbt_updated_at,
    md5(id || '|' || updated_at::text) as scd_id
from simple_archive_004.seed
where id >= 10 and id <= 20;


-- insert 10 new records
insert into simple_archive_004.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (21, 'Judy', 'Robinson', 'jrobinsonk@blogs.com', 'Female', '208.21.192.232', '2016-09-18 08:27:38');
insert into simple_archive_004.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (22, 'Kevin', 'Alvarez', 'kalvarezl@buzzfeed.com', 'Male', '228.106.146.9', '2016-07-29 03:07:37');
insert into simple_archive_004.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (23, 'Barbara', 'Carr', 'bcarrm@pen.io', 'Female', '106.165.140.17', '2015-09-24 13:27:23');
insert into simple_archive_004.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (24, 'William', 'Watkins', 'wwatkinsn@guardian.co.uk', 'Male', '78.155.84.6', '2016-03-08 19:13:08');
insert into simple_archive_004.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (25, 'Judy', 'Cooper', 'jcoopero@google.com.au', 'Female', '24.149.123.184', '2016-10-05 20:49:33');
insert into simple_archive_004.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (26, 'Shirley', 'Castillo', 'scastillop@samsung.com', 'Female', '129.252.181.12', '2016-06-20 21:12:21');
insert into simple_archive_004.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (27, 'Justin', 'Harper', 'jharperq@opera.com', 'Male', '131.172.103.218', '2016-05-21 22:56:46');
insert into simple_archive_004.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (28, 'Marie', 'Medina', 'mmedinar@nhs.uk', 'Female', '188.119.125.67', '2015-10-08 13:44:33');
insert into simple_archive_004.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (29, 'Kelly', 'Edwards', 'kedwardss@phoca.cz', 'Female', '47.121.157.66', '2015-09-15 06:33:37');
insert into simple_archive_004.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values (30, 'Carl', 'Coleman', 'ccolemant@wikipedia.org', 'Male', '82.227.154.83', '2016-05-26 16:46:40');


-- add these new records to the archive table
insert into simple_archive_004.archive_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    valid_from,
    valid_to,
    dbt_updated_at,
    scd_id
)

select
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    -- fields added by archival
    updated_at as valid_from,
    null::timestamp as valid_to,
    updated_at as dbt_updated_at,
    md5(id || '|' || updated_at::text) as scd_id
from simple_archive_004.seed
where id > 20;
