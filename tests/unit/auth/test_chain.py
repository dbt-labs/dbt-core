import os
from unittest import mock

import pytest

from dbt.auth.chain import AuthChain
from dbt.auth.resolvers import CloudYamlResolver, EnvVarResolver
from dbt.exceptions import MalformedAuthConfig, NotAuthenticated


class TestAuthChainResolve:
    def test_returns_first_successful_credential(self):
        env = {
            "DBT_CLOUD_ACCOUNT_HOST": "ab123.us1.dbt.com",
            "DBT_CLOUD_TOKEN": "dbtc_abc123",
            "DBT_CLOUD_ACCOUNT_ID": "42",
        }
        chain = AuthChain([EnvVarResolver()])
        with mock.patch.dict(os.environ, env, clear=False):
            cred = chain.resolve()
        assert cred.token == "dbtc_abc123"

    def test_skips_not_authenticated_tries_next(self, tmp_path):
        p = tmp_path / "dbt_cloud.yml"
        p.write_text(
            """
version: "1"
context:
  active-project: "proj-1"
  active-host: "ab123.us1.dbt.com"
projects:
  - project-id: "proj-1"
    project-name: "Test"
    account-name: "acme"
    account-id: "42"
    account-host: "ab123.us1.dbt.com"
    token-name: "tok"
    token-value: "dbtc_from_yaml"
"""
        )
        chain = AuthChain(
            [
                EnvVarResolver(),
                CloudYamlResolver(path=p),
            ]
        )
        with mock.patch.dict(os.environ, {}, clear=True):
            cred = chain.resolve()
        assert cred.token == "dbtc_from_yaml"

    def test_continues_past_errors_returns_first_error(self, tmp_path):
        bad = tmp_path / "bad.yml"
        bad.write_text("not: valid: yaml: [[[")
        missing = tmp_path / "missing.yml"

        chain = AuthChain(
            [
                CloudYamlResolver(path=bad),
                CloudYamlResolver(path=missing),
            ]
        )
        with pytest.raises(MalformedAuthConfig):
            chain.resolve()

    def test_continues_past_error_succeeds_on_next(self, tmp_path):
        bad = tmp_path / "bad.yml"
        bad.write_text("not: valid: yaml: [[[")
        good = tmp_path / "good.yml"
        good.write_text(
            """
version: "1"
context:
  active-project: "proj-1"
  active-host: "ab123.us1.dbt.com"
projects:
  - project-id: "proj-1"
    project-name: "Test"
    account-name: "acme"
    account-id: "42"
    account-host: "ab123.us1.dbt.com"
    token-name: "tok"
    token-value: "dbtc_good"
"""
        )
        chain = AuthChain(
            [
                CloudYamlResolver(path=bad),
                CloudYamlResolver(path=good),
            ]
        )
        cred = chain.resolve()
        assert cred.token == "dbtc_good"

    def test_all_not_authenticated_raises_not_authenticated(self):
        chain = AuthChain([EnvVarResolver()])
        with mock.patch.dict(os.environ, {}, clear=True):
            with pytest.raises(NotAuthenticated):
                chain.resolve()

    def test_empty_chain_raises_not_authenticated(self):
        chain = AuthChain([])
        with pytest.raises(NotAuthenticated):
            chain.resolve()
