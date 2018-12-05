
create view {schema}.materialized as (
    select 1 as id
);

create table {schema}.materialized__dbt_backup (
	id BIGSERIAL PRIMARY KEY
);
