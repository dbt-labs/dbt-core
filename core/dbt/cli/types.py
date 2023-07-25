from enum import Enum
from typing import List

from dbt.exceptions import DbtInternalError


class Command(Enum):
    BUILD = "build"
    CLEAN = "clean"
    COMPILE = "compile"
    CLONE = "clone"
    DOCS_GENERATE = "generate"
    DOCS_SERVE = "serve"
    DEBUG = "debug"
    DEPS = "deps"
    INIT = "init"
    LIST = "list"
    PARSE = "parse"
    RUN = "run"
    RUN_OPERATION = "run-operation"
    SEED = "seed"
    SHOW = "show"
    SNAPSHOT = "snapshot"
    SOURCE_FRESHNESS = "freshness"
    TEST = "test"
    RETRY = "retry"

    @classmethod
    def from_str(cls, s: str) -> "Command":
        try:
            return cls(s)
        except ValueError:
            raise DbtInternalError(f"No value '{s}' exists in Command enum")

    def to_list(self) -> List[str]:
        return {
            Command.DOCS_GENERATE: ["docs", "generate"],
            Command.DOCS_SERVE: ["docs", "serve"],
            Command.SOURCE_FRESHNESS: ["source", "freshness"],
        }.get(self, [self.value])
