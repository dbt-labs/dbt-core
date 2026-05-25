from dbt.auth import OAUTH_CLIENT_ID
from dbt.auth.errors import AuthError
from dbt.auth.session_cache import (
    DEFAULT_CACHE_PATH,
    read_session_cache,
    remove_session,
)
from dbt.cli.flags import Flags
from dbt.task.base import BaseTask
from dbt_common.events.functions import fire_event
from dbt_common.events.types import Note


class LogoutTask(BaseTask):
    def __init__(self, args: Flags) -> None:
        super().__init__(args)

    def run(self):
        fire_event(Note(msg="Starting dbt logout..."))

        try:
            cache = read_session_cache(DEFAULT_CACHE_PATH)
        except AuthError as e:
            fire_event(Note(msg=f"Could not read session cache: {e}"))
            return False

        client_sessions = [s for s in cache.sessions if s.client_id == OAUTH_CLIENT_ID]

        if not client_sessions:
            fire_event(
                Note(
                    msg="No active OAuth session found. If you are using a PAT or service token via dbt_cloud.yml, remove it manually."
                )
            )
            return True

        for session in client_sessions:
            remove_session(OAUTH_CLIENT_ID, session.account_id, DEFAULT_CACHE_PATH)

        fire_event(Note(msg="Logged out."))

        return True

    def interpret_results(self, results):
        return results
