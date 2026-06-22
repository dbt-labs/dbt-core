"""PEP 517 build backend for the dbt download-at-install sdist.

When pip builds from this sdist, ``build_wheel`` downloads the prebuilt wheel
for the user's platform from the release's asset store, verifies its sha256
against the embedded ``assets.json``, and hands it back — nothing is compiled.
Uses only the stdlib plus ``packaging`` (declared in pyproject build-requires).
"""

import hashlib
import json
import time
import urllib.error
import urllib.request
from pathlib import Path

from packaging.tags import sys_tags

_HERE = Path(__file__).resolve().parent
_MANIFEST = json.loads((_HERE / "assets.json").read_text(encoding="utf-8"))

_RETRIES = 4
_TIMEOUT = 60


def _select_wheel():
    """Pick the manifest wheel for the first platform tag this machine accepts.

    ``sys_tags()`` is pip's ordered list of compatible tags, so the most specific
    build wins.
    """
    wheels = _MANIFEST["wheels"]
    for tag in sys_tags():
        entry = wheels.get(tag.platform)
        if entry is not None:
            return entry
    raise RuntimeError(
        "no prebuilt {name} {ver} wheel for this platform; "
        "available platforms: {plats}".format(
            name=_MANIFEST["name"],
            ver=_MANIFEST["version"],
            plats=", ".join(sorted(wheels)),
        )
    )


def _fetch(url):
    last = None
    for attempt in range(1, _RETRIES + 1):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "{}-sdist".format(_MANIFEST["name"])}
            )
            with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
                return resp.read()
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last = exc
            if attempt < _RETRIES:
                time.sleep(2 ** (attempt - 1))
    raise RuntimeError(f"failed to download {url}: {last}")


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    entry = _select_wheel()
    filename = entry["filename"]
    url = "{base}/{file}".format(base=_MANIFEST["base_url"].rstrip("/"), file=filename)
    data = _fetch(url)

    digest = hashlib.sha256(data).hexdigest()
    if digest != entry["sha256"]:
        raise RuntimeError(
            f"sha256 mismatch for {filename}: expected {entry['sha256']}, got {digest}"
        )

    out = Path(wheel_directory) / filename
    out.write_bytes(data)
    return filename


def get_requires_for_build_wheel(config_settings=None):
    return []


def build_sdist(sdist_directory, config_settings=None):
    # The sdist is produced by `dbt-ci pypi pack --sdist`, not by this backend.
    raise RuntimeError(
        "this backend does not build sdists; use `dbt-ci pypi pack --sdist`"
    )
