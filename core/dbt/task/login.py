import requests

from dbt.auth import AuthChain, AuthError
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

        _warm_license_cache(credential)
        fire_event(
            Note(msg=f"Logged in as {credential.account_host} (account {credential.account_id}).")
        )
        return True

    def interpret_results(self, results):
        return results


def _warm_license_cache(credential) -> None:
    """Best-effort POST to warm the feature license cache. Non-fatal on failure."""
    url = (
        f"https://{credential.account_host}"
        f"/api/private/accounts/{credential.account_id}/feature-licenses/"
    )
    try:
        requests.post(
            url,
            headers={"Authorization": f"Bearer {credential.token}"},
            timeout=5,
        )
    except requests.RequestException:
        pass
