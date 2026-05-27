import os
from unittest import mock

import pytest

from dbt.auth.chain import OAUTH_CLIENT_ID, AuthChain
from dbt.auth.resolvers import CloudYamlResolver, EnvVarResolver, ResolverKind
from dbt.exceptions import Malformed, NotAuthenticated


class TestAuthChainConstructors:
    def test_default_has_three_resolvers(self):
        chain = AuthChain.default()
        assert len(chain._resolvers) == 3

    def test_default_resolver_order(self):
        chain = AuthChain.default()
        kinds = [r.kind for r in chain._resolvers]
        assert kinds == [
            ResolverKind.ENV_VAR,
            ResolverKind.OAUTH_PASSIVE,
            ResolverKind.CLOUD_YAML,
        ]

    def test_interactive_has_four_resolvers(self):
        chain = AuthChain.interactive()
        assert len(chain._resolvers) == 4

    def test_interactive_includes_interactive_resolver(self):
        chain = AuthChain.interactive()
        kinds = [r.kind for r in chain._resolvers]
        assert ResolverKind.OAUTH_INTERACTIVE in kinds

    def test_default_excludes_interactive_resolver(self):
        chain = AuthChain.default()
        kinds = [r.kind for r in chain._resolvers]
        assert ResolverKind.OAUTH_INTERACTIVE not in kinds


class TestAuthChainResolve:
    def test_returns_first_successful_credential(self):
        """EnvVar resolver succeeds, chain returns immediately."""
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
        """EnvVar fails (not authenticated), CloudYaml succeeds."""
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
        """Malformed YAML (error) followed by missing file (not authenticated).
        Chain returns the Malformed error since it was the first non-NotAuthenticated error."""
        bad = tmp_path / "bad.yml"
        bad.write_text("not: valid: yaml: [[[")
        missing = tmp_path / "missing.yml"

        chain = AuthChain(
            [
                CloudYamlResolver(path=bad),
                CloudYamlResolver(path=missing),
            ]
        )
        with pytest.raises(Malformed):
            chain.resolve()

    def test_continues_past_error_succeeds_on_next(self, tmp_path):
        """Malformed YAML (error) followed by valid YAML (success)."""
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


class TestOAuthClientId:
    def test_client_id_is_expected_value(self):
        assert OAUTH_CLIENT_ID == "854ad54c885f03bbe6ca7eb1e75593fb"
