import time

from dbt.auth import AuthChain, AuthError, ResolverKind
from dbt.auth.credentials import PlatformCredential, StateCredential
from dbt.auth.oauth.platform import on_platform_login_success
from dbt.auth.oauth.state import on_state_login_success
from dbt.cli.flags import Flags
from dbt.exceptions import (
    AuthenticationExpired,
    InaccessibleSource,
    MalformedAuthConfig,
    NotAuthenticated,
)
from dbt.task.base import BaseTask
from dbt_common.events.functions import fire_event
from dbt_common.events.types import Note


class LoginTask(BaseTask):
    def __init__(self, args: Flags) -> None:
        super().__init__(args)

    def run(self):
        try:
            chain = AuthChain.interactive()
            credential = chain.resolve()
        except AuthError as e:
            fire_event(Note(msg=f"Authentication failed. Re-run dbt login to try again.\n\n{e}"))
            return False

        if isinstance(credential, StateCredential):
            on_state_login_success(credential)
        elif isinstance(credential, PlatformCredential):
            on_platform_login_success(credential)

        return True

    def interpret_results(self, results):
        return results


class LoginStatusTask(BaseTask):
    SOURCE_LABELS = {
        ResolverKind.ENV_VAR: "environment variables",
        ResolverKind.OAUTH_PASSIVE: "OAuth",
        ResolverKind.OAUTH_INTERACTIVE: "OAuth",
        ResolverKind.CLOUD_YAML: "dbt_cloud.yml",
    }

    def __init__(self, args: Flags) -> None:
        super().__init__(args)

    @staticmethod
    def _format_expiry(expires_at: float) -> str:
        remaining = expires_at - time.time()
        if remaining <= 0:
            return "expired — run `dbt login` to re-authenticate"
        total_secs = int(remaining)
        hours = total_secs // 3600
        mins = (total_secs % 3600) // 60
        if hours > 0:
            return f"in {hours}h {mins}m"
        return f"in {mins}m"

    def run(self):
        try:
            chain = AuthChain.default()
            credential, source = chain.resolve_with_source()
        except NotAuthenticated:
            fire_event(
                Note(
                    msg="Status: unauthenticated\n"
                    "  sources checked (in order):\n"
                    "    1. env vars:       DBT_CLOUD_ACCOUNT_HOST, DBT_CLOUD_TOKEN, DBT_CLOUD_ACCOUNT_ID\n"
                    "    2. OAuth session:  ~/.dbt/oauth_sessions.json  (run `dbt login` to create one)\n"
                    "    3. dbt_cloud.yml:  ./dbt_cloud.yml or ~/.dbt/dbt_cloud.yml"
                )
            )
            return False
        except AuthenticationExpired:
            fire_event(
                Note(
                    msg="Status: unauthenticated (credentials expired)\n"
                    "  run `dbt login` to re-authenticate"
                )
            )
            return False
        except InaccessibleSource as e:
            fire_event(
                Note(msg=f"Status: unauthenticated (could not read credential source: {e})")
            )
            return False
        except MalformedAuthConfig as e:
            fire_event(Note(msg=f"Status: unauthenticated (credential source is invalid: {e})"))
            return False
        except AuthError as e:
            fire_event(Note(msg=f"Status: unauthenticated ({e})"))
            return False

        via = self.SOURCE_LABELS.get(source, str(source.value))

        if isinstance(credential, PlatformCredential):
            if credential.oauth_session:
                s = credential.oauth_session
                fire_event(
                    Note(
                        msg=f"Status: authenticated (via {via})\n"
                        f"  account host:  {s.account_host}\n"
                        f"  account ID:    {s.account_id}\n"
                        f"  user ID:       {s.user_id}\n"
                        f"  expires:       {self._format_expiry(s.expires_at)}"
                    )
                )
            else:
                fire_event(
                    Note(
                        msg=f"Status: authenticated (via {via})\n"
                        f"  account host:  {credential.account_host}\n"
                        f"  account ID:    {credential.account_id}"
                    )
                )
        else:
            fire_event(Note(msg=f"Status: authenticated (via {via})"))

        return True

    def interpret_results(self, results):
        return results
