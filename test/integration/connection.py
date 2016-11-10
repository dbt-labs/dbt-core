import psycopg2

handle = psycopg2.connect(
    "dbname='dbt' user='root' host='database' password='password' port='5432' connect_timeout=3"
)
