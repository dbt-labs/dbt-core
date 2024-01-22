import re
from typing import Dict

from dbt_common.exceptions import DbtInternalError


class CacheInconsistencyError(DbtInternalError):
    def __init__(self, msg: str):
        self.msg = msg
        formatted_msg = f"Cache inconsistency detected: {self.msg}"
        super().__init__(msg=formatted_msg)


class NewNameAlreadyInCacheError(CacheInconsistencyError):
    def __init__(self, old_key: str, new_key: str):
        self.old_key = old_key
        self.new_key = new_key
        msg = (
            f'in rename of "{self.old_key}" -> "{self.new_key}", new name is in the cache already'
        )
        super().__init__(msg)


class ReferencedLinkNotCachedError(CacheInconsistencyError):
    def __init__(self, referenced_key: str):
        self.referenced_key = referenced_key
        msg = f"in add_link, referenced link key {self.referenced_key} not in cache!"
        super().__init__(msg)


class DependentLinkNotCachedError(CacheInconsistencyError):
    def __init__(self, dependent_key: str):
        self.dependent_key = dependent_key
        msg = f"in add_link, dependent link key {self.dependent_key} not in cache!"
        super().__init__(msg)


class TruncatedModelNameCausedCollisionError(CacheInconsistencyError):
    def __init__(self, new_key, relations: Dict):
        self.new_key = new_key
        self.relations = relations
        super().__init__(self.get_message())

    def get_message(self) -> str:
        # Tell user when collision caused by model names truncated during
        # materialization.
        match = re.search("__dbt_backup|__dbt_tmp$", self.new_key.identifier)
        if match:
            truncated_model_name_prefix = self.new_key.identifier[: match.start()]
            message_addendum = (
                "\n\nName collisions can occur when the length of two "
                "models' names approach your database's builtin limit. "
                "Try restructuring your project such that no two models "
                f"share the prefix '{truncated_model_name_prefix}'. "
                "Then, clean your warehouse of any removed models."
            )
        else:
            message_addendum = ""

        msg = f"in rename, new key {self.new_key} already in cache: {list(self.relations.keys())}{message_addendum}"

        return msg


class NoneRelationFoundError(CacheInconsistencyError):
    def __init__(self):
        msg = "in get_relations, a None relation was found in the cache!"
        super().__init__(msg)
