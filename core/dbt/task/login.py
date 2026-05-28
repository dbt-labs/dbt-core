from dbt.auth import AuthChain, AuthError
from dbt.auth.credentials import PlatformCredential, StateCredential
from dbt.auth.oauth.platform import on_platform_login_success
from dbt.auth.oauth.state import on_state_login_success
from dbt.cli.flags import Flags
from dbt.task.base import BaseTask
from dbt_common.events.functions import fire_event
from dbt_common.events.types import Note


class LoginTask(BaseTask):
    def __init__(self, args: Flags) -> None:
        super().__init__(args)

    def run(self):
        fire_event(Note(msg="Starting dbt login..."))
        try:
            chain = AuthChain.interactive()
            credential = chain.resolve()
        except AuthError as e:
            fire_event(Note(msg=f"Login failed: {e}"))
            return False

        if isinstance(credential, StateCredential):
            on_state_login_success(credential)
        elif isinstance(credential, PlatformCredential):
            on_platform_login_success(credential)

        return True

    def interpret_results(self, results):
        return results
