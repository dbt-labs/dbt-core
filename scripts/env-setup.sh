#!/bin/bash
# Set environment variables required for integration tests
# This is used in the release workflow to set the environment variables for the integration tests
echo "DBT_INVOCATION_ENV=github-actions" >> $GITHUB_ENV
echo "DBT_TEST_USER_1=dbt_test_user_1" >> $GITHUB_ENV
echo "DBT_TEST_USER_2=dbt_test_user_2" >> $GITHUB_ENV
echo "DBT_TEST_USER_3=dbt_test_user_3" >> $GITHUB_ENV
