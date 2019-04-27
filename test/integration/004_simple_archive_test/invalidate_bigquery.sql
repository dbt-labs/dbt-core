
-- update records 11 - 21. Change email and updated_at field
update {database}.{schema}.seed set
    updated_at = timestamp_add(updated_at, interval 1 hour),
    email      = concat('new_', email)
where id >= 10 and id <= 20;


-- invalidate records 11 - 21
update {database}.{schema}.archive_expected set
    valid_to   = timestamp_add(updated_at, interval 1 hour)
where id >= 10 and id <= 20;
