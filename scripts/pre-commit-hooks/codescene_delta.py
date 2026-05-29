#!/usr/bin/env python3
# Optional hook: skip silently unless `cs` is installed AND CodeScene auth is configured.
# When active, `cs delta --git-hook` exits 1 on Code Health findings, blocking the commit.
import os
import shutil
import subprocess
import sys


def _has_auth() -> bool:
    if os.environ.get("CS_ACCESS_TOKEN"):
        return True
    return all(os.environ.get(var) for var in ("CS_ONPREM_API_URL", "CS_USERNAME", "CS_PASSWORD"))


def main() -> int:
    if shutil.which("cs") is None:
        return 0
    if not _has_auth():
        return 0
    return subprocess.run(["cs", "delta", "--git-hook", "--staged"]).returncode


if __name__ == "__main__":
    sys.exit(main())
