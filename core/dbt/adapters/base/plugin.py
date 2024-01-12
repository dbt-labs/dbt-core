from typing import List, Optional, Type
from pathlib import Path

from dbt.adapters.base import Credentials
from dbt.adapters.protocol import AdapterProtocol


class AdapterPlugin:
    """Defines the basic requirements for a dbt adapter plugin.

    :param include_path: The path to this adapter plugin's root
    :param dependencies: A list of adapter names that this adapter depends
        upon.
    """

    def __init__(
        self,
        adapter: Type[AdapterProtocol],
        credentials: Type[Credentials],
        include_path: str,
        dependencies: Optional[List[str]] = None,
        project_name: Optional[str] = None,
    ) -> None:

        self.adapter: Type[AdapterProtocol] = adapter
        self.credentials: Type[Credentials] = credentials
        self.include_path: str = include_path
        self.project_name: str = project_name or f"dbt_{Path(include_path).name}"
        self.dependencies: List[str]
        if dependencies is None:
            self.dependencies = []
        else:
            self.dependencies = dependencies
