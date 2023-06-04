from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Hashable, Optional

from dbt.adapters.relation_configs.config_base import RelationConfigBase
from dbt.dataclass_schema import StrEnum


class RelationConfigChangeAction(StrEnum):
    alter = "alter"
    create = "create"
    drop = "drop"


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class RelationConfigChange(RelationConfigBase, ABC):
    action: Optional[RelationConfigChangeAction] = None
    context: Hashable = (
        None  # this is usually a RelationConfig, e.g. IndexConfig, but shouldn't be limited
    )

    @property
    @abstractmethod
    def requires_full_refresh(self) -> bool:
        raise self._not_implemented_error()

    @property
    def is_change(self) -> bool:
        return self.action is not None or self.context is not None
