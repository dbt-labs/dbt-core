REVOKE CREATE, CONNECT ON DATABASE dbt FROM noaccess;
REVOKE CREATE ON SCHEMA permission_tests_010 FROM noaccess;
DROP USER IF EXISTS noaccess;

drop schema if exists private cascade;
