from .macros import PY_36


def patch_fromisoformat():
    if PY_36:
        # noinspection PyPackageRequirements
        # noinspection PyUnresolvedReferences
        from backports.datetime_fromisoformat import MonkeyPatch

        MonkeyPatch.patch_fromisoformat()


__all__ = ["patch_fromisoformat"]
