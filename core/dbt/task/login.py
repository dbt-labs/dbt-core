import time

from dbt.auth import AuthChain, AuthError, ResolverKind
from dbt.auth.credentials import PlatformCredential, StateCredential
from dbt.auth.oauth.platform import on_platform_login_success
from dbt.auth.oauth.state import on_state_login_success
from dbt.auth.session_cache import read_state_auth
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
        credential = None
        source = None
        platform_error_msg = None

        try:
            chain = AuthChain.default()
            credential, source = chain.resolve_with_source()
        except NotAuthenticated:
            pass
        except AuthenticationExpired:
            platform_error_msg = "credentials expired — run `dbt login` to re-authenticate"
        except InaccessibleSource as e:
            platform_error_msg = f"could not read credential source: {e}"
        except MalformedAuthConfig as e:
            platform_error_msg = f"credential source is invalid: {e}"
        except AuthError as e:
            platform_error_msg = str(e)

        state_data = read_state_auth()

        if credential is None and state_data is None:
            if platform_error_msg:
                fire_event(Note(msg=f"Status: unauthenticated ({platform_error_msg})"))
            else:
                fire_event(
                    Note(
                        msg="Status: unauthenticated\n"
                        "  sources checked (in order):\n"
                        "    1. env vars:       DBT_CLOUD_ACCOUNT_HOST, DBT_CLOUD_TOKEN, DBT_CLOUD_ACCOUNT_ID\n"
                        "    2. OAuth session:  ~/.dbt/oauth_sessions.json  (run `dbt login` to create one)\n"
                        "    3. dbt_cloud.yml:  ./dbt_cloud.yml or ~/.dbt/dbt_cloud.yml\n"
                        "    4. state auth:     ~/.dbt/state_auth.json"
                    )
                )
            return False

        if credential is not None:
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

        if credential is None and state_data is not None:
            fire_event(Note(msg="Status: authenticated with dbt State"))

        return True

    def interpret_results(self, results):
        return results
