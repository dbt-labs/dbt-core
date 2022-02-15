# - VERY rudimentary test script to run latest + specific branch image builds and test them all by running `--version`
# TODO: create a real test suite

clear \
&& echo "\n\n"\
"###################################\n"\
"##### Testing dbt-core latest #####\n"\
"###################################\n"\
&& docker build --tag dbt-core \
  --target dbt-core \
  docker \
&& docker run dbt-core --version \
\
&& echo "\n\n"\
"####################################\n"\
"##### Testing dbt-core-1.0.0b1 #####\n"\
"####################################\n"\
&& docker build --tag dbt-core-1.0.0b1 \
  --target dbt-core \
  --build-arg dbt_core_ref=dbt-core@v1.0.0b1 \
  docker \
&& docker run dbt-core-1.0.0b1 --version \
\
&& echo "\n\n"\
"#######################################\n"\
"##### Testing dbt-postgres latest #####\n"\
"#######################################\n"\
&& docker build --tag dbt-postgres \
  --target dbt-postgres \
  docker \
&& docker run dbt-postgres --version \
\
&& echo "\n\n"\
"########################################\n"\
"##### Testing dbt-postgres-1.0.0b1 #####\n"\
"########################################\n"\
&& docker build --tag dbt-postgres-1.0.0b1 \
  --target dbt-postgres \
  --build-arg dbt_postgres_ref=dbt-core@v1.0.0b1 \
  docker \
&& docker run dbt-postgres-1.0.0b1 --version \
\
&& echo "\n\n"\
"#######################################\n"\
"##### Testing dbt-redshift latest #####\n"\
"#######################################\n"\
&& docker build --tag dbt-redshift \
  --target dbt-redshift \
  docker \
&& docker run dbt-redshift --version \
\
&& echo "\n\n"\
"########################################\n"\
"##### Testing dbt-redshift-1.0.0b1 #####\n"\
"########################################\n"\
&& docker build --tag dbt-redshift-1.0.0b1 \
  --target dbt-redshift \
  --build-arg dbt_redshift_ref=dbt-redshift@v1.0.0b1 \
  docker \
&& docker run dbt-redshift-1.0.0b1 --version \
\
&& echo "\n\n"\
"#######################################\n"\
"##### Testing dbt-bigquery latest #####\n"\
"#######################################\n"\
&& docker build --tag dbt-bigquery \
  --target dbt-bigquery \
  docker \
&& docker run dbt-bigquery --version \
\
&& echo "\n\n"\
"########################################\n"\
"##### Testing dbt-bigquery-1.0.0b1 #####\n"\
"########################################\n"\
&& docker build --tag dbt-bigquery-1.0.0b1 \
  --target dbt-bigquery \
  --build-arg dbt_bigquery_ref=dbt-bigquery@v1.0.0b1 \
  docker \
&& docker run dbt-bigquery-1.0.0b1 --version \
\
&& echo "\n\n"\
"########################################\n"\
"##### Testing dbt-snowflake latest #####\n"\
"########################################\n"\
&& docker build --tag dbt-snowflake \
  --target dbt-snowflake \
  docker \
&& docker run dbt-snowflake --version \
\
&& echo "\n\n"\
"#########################################\n"\
"##### Testing dbt-snowflake-1.0.0b1 #####\n"\
"#########################################\n"\
&& docker build --tag dbt-snowflake-1.0.0b1 \
  --target dbt-snowflake\
  --build-arg dbt_snowflake_ref=dbt-snowflake@v1.0.0b1 \
  docker \
&& docker run dbt-snowflake-1.0.0b1 --version \
\
&& echo "\n\n"\
"####################################\n"\
"##### Testing dbt-spark latest #####\n"\
"####################################\n"\
&& docker build --tag dbt-spark \
  --target dbt-spark \
  docker \
&& docker run dbt-spark --version \
\
&& echo "\n\n"\
"#####################################\n"\
"##### Testing dbt-spark-1.0.0rc2 ####\n"\
"#####################################\n"\
&& docker build --tag dbt-spark-1.0.0rc2 \
  --target dbt-spark \
  --build-arg dbt_spark_ref=dbt-spark@v1.0.0rc2 \
  docker \
&& docker run dbt-spark-1.0.0rc2 --version \
\
&& echo "\n\n"\
"###########################\n"\
"##### Testing dbt-all #####\n"\
"###########################\n"\
&& docker build --tag dbt-all \
  --target dbt-all \
  docker \
&& docker run dbt-all --version \
\
&& echo "\n\n"\
"##########################################\n"\
"##### Testing third party db adapter #####\n"\
"##########################################\n"\
&& docker build --tag dbt-materialize \
  --target dbt-third-party \
  --build-arg dbt_third_party="dbt-materialize" \
  docker \
&& docker run dbt-materialize --version
