from typing import Mapping, Any

from dbt.exceptions import DbtValidationError


class AliasError(DbtValidationError):
    pass


# core level exceptions
class DuplicateAliasError(AliasError):
    def __init__(self, kwargs: Mapping[str, Any], aliases: Mapping[str, str], canonical_key: str):
        self.kwargs = kwargs
        self.aliases = aliases
        self.canonical_key = canonical_key
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        # dupe found: go through the dict so we can have a nice-ish error
        key_names = ", ".join(
            "{}".format(k) for k in self.kwargs if self.aliases.get(k) == self.canonical_key
        )
        msg = f'Got duplicate keys: ({key_names}) all map to "{self.canonical_key}"'
        return msg
