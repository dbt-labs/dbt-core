"""Project-level configuration for rationale enforcement.

Reads a `.rationale.yml` file from the dbt project root to control
enforcement level, score thresholds, and resource-type scoping.
When no config file is present, sensible defaults are used.
"""

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

CONFIG_FILENAME = ".rationale.yml"


class EnforcementLevel(str, Enum):
    """How strictly to enforce rationale requirements."""
    OFF = "off"         # Report only, always exit 0
    SOFT = "soft"       # Warn on missing/invalid, exit 0 (CI annotations only)
    HARD = "hard"       # Fail on missing required fields or validation errors


@dataclass
class Config:
    """Rationale analyzer configuration."""
    # Enforcement
    enforcement: EnforcementLevel = EnforcementLevel.SOFT

    # Minimum aggregate quality score (0-100) to pass CI in hard mode
    min_score: float = 0.0

    # Minimum coverage percentage to pass CI in hard mode
    min_coverage: float = 0.0

    # Resource types to check (empty = all supported types)
    resource_types: List[str] = field(default_factory=list)

    # Number of days before exception expiry to start warning
    exception_warn_days: int = 30

    # Paths to exclude from scanning (relative to project root)
    exclude_paths: List[str] = field(default_factory=list)

    @property
    def exit_code_on_failure(self) -> int:
        """Exit code when enforcement criteria are not met."""
        if self.enforcement == EnforcementLevel.HARD:
            return 1
        return 0


def load_config(project_path: str) -> Config:
    """Load configuration from .rationale.yml in the project root.

    Returns default Config if file doesn't exist.
    """
    config_file = Path(project_path) / CONFIG_FILENAME

    if not config_file.exists():
        return Config()

    try:
        with open(config_file, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except (yaml.YAMLError, OSError):
        return Config()

    if not isinstance(data, dict):
        return Config()

    return _parse_config(data)


def _parse_config(data: Dict[str, Any]) -> Config:
    """Parse a config dict into a Config object."""
    config = Config()

    # enforcement
    enforcement = data.get("enforcement", "soft")
    try:
        config.enforcement = EnforcementLevel(str(enforcement).lower())
    except ValueError:
        config.enforcement = EnforcementLevel.SOFT

    # min_score
    min_score = data.get("min_score")
    if isinstance(min_score, (int, float)):
        config.min_score = max(0.0, min(100.0, float(min_score)))

    # min_coverage
    min_coverage = data.get("min_coverage")
    if isinstance(min_coverage, (int, float)):
        config.min_coverage = max(0.0, min(100.0, float(min_coverage)))

    # resource_types
    resource_types = data.get("resource_types")
    if isinstance(resource_types, list):
        config.resource_types = [str(rt) for rt in resource_types]

    # exception_warn_days
    warn_days = data.get("exception_warn_days")
    if isinstance(warn_days, int):
        config.exception_warn_days = max(0, warn_days)

    # exclude_paths
    exclude = data.get("exclude_paths")
    if isinstance(exclude, list):
        config.exclude_paths = [str(p) for p in exclude]

    return config
