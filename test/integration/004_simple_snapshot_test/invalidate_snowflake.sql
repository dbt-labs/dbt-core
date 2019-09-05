
-- update records 11 - 21. Change email and updated_at field
update {database}.{schema}.seed set
    updated_at = DATEADD(hour, 1, updated_at),
    email      = case when id = 20 then 'pfoxj@creativecommons.org' else 'new_' || email end
where id >= 10 and id <= 20;


-- invalidate records 11 - 21
update {database}.{schema}.snapshot_expected set
    dbt_valid_to   = DATEADD(hour, 1, updated_at)
where id >= 10 and id <= 20;
