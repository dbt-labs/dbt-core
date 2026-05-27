import requests

from dbt.auth import AuthChain, AuthError
from dbt.cli.flags import Flags
from dbt.config.user_settings import set_user_setting_flag
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

        if credential is None:
            fire_event(Note(msg="dbt State login successful."))
            return True

        _warm_license_cache(credential)
        fire_event(
            Note(msg=f"Logged in as {credential.account_host} (account {credential.account_id}).")
        )

        configured = _is_state_configured(credential)
        enabled = getattr(self.args, "MANAGE_STATE", False) or False
        _post_platform_login(configured=configured, enabled=enabled)

        return True

    def interpret_results(self, results):
        return results


def _post_platform_login(configured: bool, enabled: bool) -> None:
    if configured and enabled:
        return
    if not configured and not enabled:
        return
    if configured and not enabled:
        set_user_setting_flag("manage_state", True)
        fire_event(Note(msg="dbt State is available for your account — enabled locally."))
        return
    # enabled but not configured
    fire_event(
        Note(
            msg=(
                "dbt State is enabled locally (manage_state: true) "
                "but is not configured for your account.\n"
                "Contact your account administrator to set up dbt State, "
                "or visit https://docs.getdbt.com/docs/dbt-state"
            )
        )
    )


def _is_state_configured(credential) -> bool:
    url = (
        f"https://{credential.account_host}"
        f"/api/private/accounts/{credential.account_id}/features/"
    )
    try:
        resp = requests.get(
            url,
            headers={"Authorization": f"Bearer {credential.token}"},
            timeout=5,
        )
        resp.raise_for_status()
        return resp.json().get("data", {}).get("dbt-state", False) is True
    except (requests.RequestException, ValueError, KeyError):
        return False


def _warm_license_cache(credential) -> None:
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
