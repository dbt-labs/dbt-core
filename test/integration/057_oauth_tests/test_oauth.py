"""
The first time using an account for testing, you should run this:

```
CREATE OR REPLACE SECURITY INTEGRATION DBT_INTEGRATION_TEST_OAUTH
  TYPE = OAUTH
  ENABLED = TRUE
  OAUTH_CLIENT = CUSTOM
  OAUTH_CLIENT_TYPE = 'CONFIDENTIAL'
  OAUTH_REDIRECT_URI = 'http://localhost:8080'
  oauth_issue_refresh_tokens = true
  OAUTH_ALLOW_NON_TLS_REDIRECT_URI = true
  BLOCKED_ROLES_LIST = <everything but your integration test role goes here: ('ACCOUNTADMIN', 'SECURITYADMIN'), or ignore it if you don't care>
  oauth_refresh_token_validity = 7776000;
```


Every month (or any amount <90 days):

Run `select SYSTEM$SHOW_OAUTH_CLIENT_SECRETS('DBT_INTEGRATION_TEST_OAUTH');`

The only row/column of output should be a json blob, it goes (within single
quotes!) as the second argument to the server script:

python scripts/werkzeug-refresh-token.py ${acount_name} '${json_blob}'

Open http://localhost:8080

Log in as the test user, get a resonse page with some environment variables.
Update CI providers and test.env with the new values (If you kept the security
integration the same, just the refresh token changed)
"""
import os
import pytest
from test.integration.base import DBTIntegrationTest, use_profile


def env_set_truthy(key):
    """Return the value if it was set to a "truthy" string value, or None
    otherwise.
    """
    value = os.getenv(key)
    if not value or value.lower() in ('0', 'false', 'f'):
        return None
    return value


OAUTH_TESTS_DISABLED = env_set_truthy('DBT_INTEGRATION_TEST_SNOWFLAKE_OAUTH_DISABLED')


class TestSnowflakeOauth(DBTIntegrationTest):
    @property
    def schema(self):
        return "simple_copy_001"

    @staticmethod
    def dir(path):
        return path.lstrip('/')

    @property
    def models(self):
        return self.dir("models")

    def snowflake_profile(self):
        profile = super().snowflake_profile()
        profile['test']['target'] = 'oauth'
        missing = ', '.join(
            k for k in ('oauth_client_id', 'oauth_client_secret', 'token')
            if k not in profile['test']['outputs']['oauth']
        )
        if missing:
            raise ValueError(f'Cannot run test - {missing} not configured')
        del profile['test']['outputs']['default2']
        del profile['test']['outputs']['noaccess']
        return profile

    @pytest.mark.skipif(OAUTH_TESTS_DISABLED, reason='oauth tests disabled')
    @use_profile('snowflake')
    def test_snowflake_basic(self):
        self.run_dbt()
        self.assertManyRelationsEqual([['MODEL_3'], ['MODEL_4']])
