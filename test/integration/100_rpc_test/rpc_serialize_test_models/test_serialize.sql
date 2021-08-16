
-- see https://github.com/dbt-labs/dbt/issues/3464
-- Make sure that Undefined values are serialized correctly
-- in RPC responses
{{ log(none['foo']) }}

select 1 as id

