
-- update records 11 - 21. Change email and updated_at field
update {schema}.seed set
    updated_at = updated_at + interval '1 hour',
    email      = 'new_' || email
where id >= 10 and id <= 20;


-- invalidate records 11 - 21
update {schema}.archive_expected set
    dbt_valid_to   = updated_at + interval '1 hour'
where id >= 10 and id <= 20;


update {schema}.archive_castillo_expected set
    dbt_valid_to   = updated_at + interval '1 hour'
where id >= 10 and id <= 20;


update {schema}.archive_alvarez_expected set
    dbt_valid_to   = updated_at + interval '1 hour'
where id >= 10 and id <= 20;


update {schema}.archive_kelly_expected set
    dbt_valid_to   = updated_at + interval '1 hour'
where id >= 10 and id <= 20;
