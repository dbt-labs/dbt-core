
createdb dbt
psql -c "CREATE ROLE IF NOT EXISTS root WITH UNENCRYPTED PASSWORD 'password';" -U postgres
psql -c "ALTER ROLE root WITH LOGIN;" -U postgres
psql -c "GRANT CREATE, CONNECT ON DATABASE dbt TO root;" -U postgres

psql -c "CREATE USER IF NOT EXISTS noaccess WITH UNENCRYPTED PASSWORD 'password' NOSUPERUSER;" -U postgres;
psql -c "GRANT CREATE, CONNECT ON DATABASE dbt TO noaccess;" -U postgres;
