
-- update records 11 - 21. Change email and updated_at field
update {schema}.seed set
    updated_at = updated_at + interval '1 hour',
    email      =  case when id = 20 then 'pfoxj@creativecommons.org' else 'new_' || email end
where id >= 10 and id <= 20;


-- invalidate records 11 - 21
update {schema}.snapshot_expected set
    dbt_valid_to   = updated_at + interval '1 hour'
where id >= 10 and id <= 20;


update {schema}.snapshot_castillo_expected set
    dbt_valid_to   = "1-updated_at" + interval '1 hour'
where id >= 10 and id <= 20;


update {schema}.snapshot_alvarez_expected set
    dbt_valid_to   = updated_at + interval '1 hour'
where id >= 10 and id <= 20;


update {schema}.snapshot_kelly_expected set
    dbt_valid_to   = updated_at + interval '1 hour'
where id >= 10 and id <= 20;
