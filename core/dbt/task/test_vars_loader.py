import os
import re
from typing import Any, Dict, Optional

from dbt.config.project import VARS_FILE_NAME, vars_data_from_root


def test_vars_data_from_root(tests_root: str, target: Optional[str] = None) -> Dict[str, Any]:
    """Loads only test-specific vars from tests/vars.yml and tests/vars_<target>.yml, merging them.
    Returns a dict of test vars, with target vars taking precedence over default test vars.
    """
    import yaml
    base_vars = {}
    target_vars = {}
    vars_yml_path = os.path.join(tests_root, VARS_FILE_NAME)
    if os.path.isfile(vars_yml_path):
        try:
            with open(vars_yml_path, "r") as f:
                data = yaml.safe_load(f) or {}
                if isinstance(data, dict):
                    base_vars = data.get("vars", {}) if isinstance(data.get("vars", {}), dict) else {}
        except Exception:
            # Ignore malformed or unreadable file
            base_vars = {}

    if target:
        sanitized_target = re.sub(r"[^A-Za-z0-9_\-]", "_", target)
        target_vars_filename = f"vars_{sanitized_target}.yml"
        target_vars_path = os.path.join(tests_root, target_vars_filename)
        if os.path.isfile(target_vars_path):
            try:
                with open(target_vars_path, "r") as f:
                    data = yaml.safe_load(f) or {}
                    if isinstance(data, dict):
                        target_vars = data.get("vars", {}) if isinstance(data.get("vars", {}), dict) else {}
            except Exception:
                # Ignore malformed or unreadable file
                target_vars = {}

    merged_vars = {**base_vars, **target_vars}
    return merged_vars
