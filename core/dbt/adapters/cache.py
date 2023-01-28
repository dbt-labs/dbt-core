import threading
from copy import deepcopy
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from dbt.adapters.reference_keys import (
    _make_ref_key,
    _make_ref_key_msg,
    _make_msg_from_ref_key,
    _ReferenceKey,
)
from dbt.exceptions import (
    DependentLinkNotCachedError,
    NewNameAlreadyInCacheError,
    NoneRelationFoundError,
    ReferencedLinkNotCachedError,
    TruncatedModelNameCausedCollisionError,
)
from dbt.events.functions import fire_event, fire_event_if
from dbt.events.types import CacheAction, CacheDumpGraph
import dbt.flags as flags
from dbt.utils import lowercase


def dot_separated(key: _ReferenceKey) -> str:
    """Return the key in dot-separated string form.

    :param _ReferenceKey key: The key to stringify.
    """
    return ".".join(map(str, key))


class _CachedRelation:
    """Nothing about _CachedRelation is guaranteed to be thread-safe!

    :attr str schema: The schema of this relation.
    :attr str identifier: The identifier of this relation.
    :attr Dict[_ReferenceKey, _CachedRelation] referenced_by: The relations
        that refer to this relation.
    :attr BaseRelation inner: The underlying dbt relation.
    """

    def __init__(self, inner):
        self.referenced_by = {}
        self.inner = inner

    def __str__(self) -> str:
        return ("_CachedRelation(database={}, schema={}, identifier={}, inner={})").format(
            self.database, self.schema, self.identifier, self.inner
        )

    @property
    def database(self) -> Optional[str]:
        return lowercase(self.inner.database)

    @property
    def schema(self) -> Optional[str]:
        return lowercase(self.inner.schema)

    @property
    def identifier(self) -> Optional[str]:
        return lowercase(self.inner.identifier)

    def __copy__(self):
        new = self.__class__(self.inner)
        new.__dict__.update(self.__dict__)
        return new

    def __deepcopy__(self, memo):
        new = self.__class__(self.inner.incorporate())
        new.__dict__.update(self.__dict__)
        new.referenced_by = deepcopy(self.referenced_by, memo)

    def is_referenced_by(self, key):
        return key in self.referenced_by

    def key(self):
        """Get the _ReferenceKey that represents this relation

        :return _ReferenceKey: A key for this relation.
        """
        return _make_ref_key(self)

    def add_reference(self, referrer: "_CachedRelation"):
        """Add a reference from referrer to self, indicating that if this node
        were drop...cascaded, the referrer would be dropped as well.

        :param _CachedRelation referrer: The node that refers to this node.
        """
        self.referenced_by[referrer.key()] = referrer

    def collect_consequences(self):
        """Recursively collect a set of _ReferenceKeys that would
        consequentially get dropped if this were dropped via
        "drop ... cascade".

        :return Set[_ReferenceKey]: All the relations that would be dropped
        """
        consequences = {self.key()}
        for relation in self.referenced_by.values():
            consequences.update(relation.collect_consequences())
        return consequences

    def release_references(self, keys):
        """Non-recursively indicate that an iterable of _ReferenceKey no longer
        exist. Unknown keys are ignored.

        :param Iterable[_ReferenceKey] keys: The keys to drop.
        """
        keys = set(self.referenced_by) & set(keys)
        for key in keys:
            self.referenced_by.pop(key)

    def rename(self, new_relation):
        """Rename this cached relation to new_relation.
        Note that this will change the output of key(), all refs must be
        updated!

        :param _CachedRelation new_relation: The new name to apply to the
            relation
        """
        # Relations store this stuff inside their `path` dict. But they
        # also store a table_name, and usually use it in their  .render(),
        # so we need to update that as well. It doesn't appear that
        # table_name is ever anything but the identifier (via .create())
        self.inner = self.inner.incorporate(
            path={
                "database": new_relation.inner.database,
                "schema": new_relation.inner.schema,
                "identifier": new_relation.inner.identifier,
            },
        )

    def rename_key(self, old_key, new_key):
        """Rename a reference that may or may not exist. Only handles the
        reference itself, so this is the other half of what `rename` does.

        If old_key is not in referenced_by, this is a no-op.

        :param _ReferenceKey old_key: The old key to be renamed.
        :param _ReferenceKey new_key: The new key to rename to.
        :raises InternalError: If the new key already exists.
        """
        if new_key in self.referenced_by:
            raise NewNameAlreadyInCacheError(old_key, new_key)

        if old_key not in self.referenced_by:
            return
        value = self.referenced_by.pop(old_key)
        self.referenced_by[new_key] = value

    def dump_graph_entry(self):
        """Return a key/value pair representing this key and its referents.

        return List[str]: The dot-separated form of all referent keys.
        """
        return [dot_separated(r) for r in self.referenced_by]


class RelationsCache:
    """A cache of the relations known to dbt. Keeps track of relationships
    declared between tables and handles renames/drops as a real database would.

    :attr Dict[_ReferenceKey, _CachedRelation] relations: The known relations.
    :attr threading.RLock lock: The lock around relations, held during updates.
        The adapters also hold this lock while filling the cache.
    :attr Set[str] schemas: The set of known/cached schemas, all lowercased.
    """

    def __init__(self) -> None:
        self.relations: Dict[_ReferenceKey, _CachedRelation] = {}
        self.lock = threading.RLock()
        self.schemas: Set[Tuple[Optional[str], Optional[str]]] = set()

    def add_schema(
        self,
        database: Optional[str],
        schema: Optional[str],
    ) -> None:
        """Add a schema to the set of known schemas (case-insensitive)

        :param database: The database name to add.
        :param schema: The schema name to add.
        """
        self.schemas.add((lowercase(database), lowercase(schema)))

    def drop_schema(
        self,
        database: Optional[str],
        schema: Optional[str],
    ) -> None:
        """Drop the given schema and remove it from the set of known schemas.

        Then remove all its contents (and their dependents, etc) as well.
        """
        key = (lowercase(database), lowercase(schema))
        if key not in self.schemas:
            return

        # avoid iterating over self.relations while removing things by
        # collecting the list first.

        with self.lock:
            to_remove = self._list_relations_in_schema(database, schema)
            self._remove_all(to_remove)
            # handle a drop_schema race by using discard() over remove()
            self.schemas.discard(key)

    def update_schemas(self, schemas: Iterable[Tuple[Optional[str], str]]):
        """Add multiple schemas to the set of known schemas (case-insensitive)

        :param schemas: An iterable of the schema names to add.
        """
        self.schemas.update((lowercase(d), s.lower()) for (d, s) in schemas)

    def __contains__(self, schema_id: Tuple[Optional[str], str]):
        """A schema is 'in' the relations cache if it is in the set of cached
        schemas.

        :param schema_id: The db name and schema name to look up.
        """
        db, schema = schema_id
        return (lowercase(db), schema.lower()) in self.schemas

    def dump_graph(self):
        """Dump a key-only representation of the schema to a dictionary. Every
        known relation is a key with a value of a list of keys it is referenced
        by.
        """
        # we have to hold the lock for the entire dump, if other threads modify
        # self.relations or any cache entry's referenced_by during iteration
        # it's a runtime error!
        with self.lock:
            return {dot_separated(k): v.dump_graph_entry() for k, v in self.relations.items()}

    def _setdefault(self, relation: _CachedRelation):
        """Add a relation to the cache, or return it if it already exists.

        :param _CachedRelation relation: The relation to set or get.
        :return _CachedRelation: The relation stored under the given relation's
            key
        """
        self.add_schema(relation.database, relation.schema)
        key = relation.key()
        return self.relations.setdefault(key, relation)

    def _add_link(self, referenced_key, dependent_key):
        """Add a link between two relations to the database. Both the old and
        new entries must alraedy exist in the database.

        :param _ReferenceKey referenced_key: The key identifying the referenced
            model (the one that if dropped will drop the dependent model).
        :param _ReferenceKey dependent_key: The key identifying the dependent
            model.
        :raises InternalError: If either entry does not exist.
        """
        referenced = self.relations.get(referenced_key)
        if referenced is None:
            return
        if referenced is None:
            raise ReferencedLinkNotCachedError(referenced_key)

        dependent = self.relations.get(dependent_key)
        if dependent is None:
            raise DependentLinkNotCachedError(dependent_key)

        assert dependent is not None  # we just raised!

        referenced.add_reference(dependent)

    # This is called in plugins/postgres/dbt/adapters/postgres/impl.py
    def add_link(self, referenced, dependent):
        """Add a link between two relations to the database. If either relation
        does not exist, it will be added as an "external" relation.

        The dependent model refers _to_ the referenced model. So, given
        arguments of (jake_test, bar, jake_test, foo):
        both values are in the schema jake_test and foo is a view that refers
        to bar, so "drop bar cascade" will drop foo and all of foo's
        dependents.

        :param BaseRelation referenced: The referenced model.
        :param BaseRelation dependent: The dependent model.
        :raises InternalError: If either entry does not exist.
        """
        ref_key = _make_ref_key(referenced)
        dep_key = _make_ref_key(dependent)
        if (ref_key.database, ref_key.schema) not in self:
            # if we have not cached the referenced schema at all, we must be
            # referring to a table outside our control. There's no need to make
            # a link - we will never drop the referenced relation during a run.
            fire_event(
                CacheAction(
                    ref_key=_make_msg_from_ref_key(ref_key),
                    ref_key_2=_make_msg_from_ref_key(dep_key),
                )
            )
            return
        if ref_key not in self.relations:
            # Insert a dummy "external" relation.
            referenced = referenced.replace(type=referenced.External)
            self.add(referenced)
        if dep_key not in self.relations:
            # Insert a dummy "external" relation.
            dependent = dependent.replace(type=referenced.External)
            self.add(dependent)
        fire_event(
            CacheAction(
                action="add_link",
                ref_key=_make_msg_from_ref_key(dep_key),
                ref_key_2=_make_msg_from_ref_key(ref_key),
            )
        )
        with self.lock:
            self._add_link(ref_key, dep_key)

    def add(self, relation):
        """Add the relation inner to the cache, under the schema schema and
        identifier identifier

        :param BaseRelation relation: The underlying relation.
        """
        cached = _CachedRelation(relation)
        fire_event_if(
            flags.LOG_CACHE_EVENTS,
            lambda: CacheDumpGraph(before_after="before", action="adding", dump=self.dump_graph()),
        )
        fire_event(CacheAction(action="add_relation", ref_key=_make_ref_key_msg(cached)))

        with self.lock:
            self._setdefault(cached)
        fire_event_if(
            flags.LOG_CACHE_EVENTS,
            lambda: CacheDumpGraph(before_after="after", action="adding", dump=self.dump_graph()),
        )

    def _remove_refs(self, keys):
        """Removes all references to all entries in keys. This does not
        cascade!

        :param Iterable[_ReferenceKey] keys: The keys to remove.
        """
        # remove direct refs
        for key in keys:
            del self.relations[key]
        # then remove all entries from each child
        for cached in self.relations.values():
            cached.release_references(keys)

    def drop(self, relation):
        """Drop the named relation and cascade it appropriately to all
        dependent relations.

        Because dbt proactively does many `drop relation if exist ... cascade`
        that are noops, nonexistent relation drops cause a debug log and no
        other actions.

        :param str schema: The schema of the relation to drop.
        :param str identifier: The identifier of the relation to drop.
        """
        dropped_key = _make_ref_key(relation)
        dropped_key_msg = _make_ref_key_msg(relation)
        fire_event(CacheAction(action="drop_relation", ref_key=dropped_key_msg))
        with self.lock:
            if dropped_key not in self.relations:
                fire_event(CacheAction(action="drop_missing_relation", ref_key=dropped_key_msg))
                return
            consequences = self.relations[dropped_key].collect_consequences()
            # convert from a list of _ReferenceKeys to a list of ReferenceKeyMsgs
            consequence_msgs = [_make_msg_from_ref_key(key) for key in consequences]
            fire_event(
                CacheAction(
                    action="drop_cascade", ref_key=dropped_key_msg, ref_list=consequence_msgs
                )
            )
            self._remove_refs(consequences)

    def _rename_relation(self, old_key, new_relation):
        """Rename a relation named old_key to new_key, updating references.
        Return whether or not there was a key to rename.

        :param _ReferenceKey old_key: The existing key, to rename from.
        :param _CachedRelation new_key: The new relation, to rename to.
        """
        # On the database level, a rename updates all values that were
        # previously referenced by old_name to be referenced by new_name.
        # basically, the name changes but some underlying ID moves. Kind of
        # like an object reference!
        relation = self.relations.pop(old_key)
        new_key = new_relation.key()

        # relation has to rename its innards, so it needs the _CachedRelation.
        relation.rename(new_relation)
        # update all the relations that refer to it
        for cached in self.relations.values():
            if cached.is_referenced_by(old_key):
                fire_event(
                    CacheAction(
                        action="update_reference",
                        ref_key=_make_ref_key_msg(old_key),
                        ref_key_2=_make_ref_key_msg(new_key),
                        ref_key_3=_make_ref_key_msg(cached.key()),
                    )
                )

                cached.rename_key(old_key, new_key)

        self.relations[new_key] = relation
        # also fixup the schemas!
        self.add_schema(new_key.database, new_key.schema)

        return True

    def _check_rename_constraints(self, old_key, new_key):
        """Check the rename constraints, and return whether or not the rename
        can proceed.

        If the new key is already present, that is an error.
        If the old key is absent, we debug log and return False, assuming it's
        a temp table being renamed.

        :param _ReferenceKey old_key: The existing key, to rename from.
        :param _ReferenceKey new_key: The new key, to rename to.
        :return bool: If the old relation exists for renaming.
        :raises InternalError: If the new key is already present.
        """
        if new_key in self.relations:
            # Tell user when collision caused by model names truncated during
            # materialization.
            raise TruncatedModelNameCausedCollisionError(new_key, self.relations)

        if old_key not in self.relations:
            fire_event(
                CacheAction(action="temporary_relation", ref_key=_make_msg_from_ref_key(old_key))
            )
            return False
        return True

    def rename(self, old, new):
        """Rename the old schema/identifier to the new schema/identifier and
        update references.

        If the new schema/identifier is already present, that is an error.
        If the schema/identifier key is absent, we only debug log and return,
        assuming it's a temp table being renamed.

        :param BaseRelation old: The existing relation name information.
        :param BaseRelation new: The new relation name information.
        :raises InternalError: If the new key is already present.
        """
        old_key = _make_ref_key(old)
        new_key = _make_ref_key(new)
        fire_event(
            CacheAction(
                action="rename_relation",
                ref_key=_make_msg_from_ref_key(old_key),
                ref_key_2=_make_msg_from_ref_key(new),
            )
        )

        fire_event_if(
            flags.LOG_CACHE_EVENTS,
            lambda: CacheDumpGraph(before_after="before", action="rename", dump=self.dump_graph()),
        )

        with self.lock:
            if self._check_rename_constraints(old_key, new_key):
                self._rename_relation(old_key, _CachedRelation(new))
            else:
                self._setdefault(_CachedRelation(new))

        fire_event_if(
            flags.LOG_CACHE_EVENTS,
            lambda: CacheDumpGraph(before_after="after", action="rename", dump=self.dump_graph()),
        )

    def get_relations(self, database: Optional[str], schema: Optional[str]) -> List[Any]:
        """Case-insensitively yield all relations matching the given schema.

        :param str schema: The case-insensitive schema name to list from.
        :return List[BaseRelation]: The list of relations with the given
            schema
        """
        database = lowercase(database)
        schema = lowercase(schema)
        with self.lock:
            results = [
                r.inner
                for r in self.relations.values()
                if (lowercase(r.schema) == schema and lowercase(r.database) == database)
            ]

        if None in results:
            raise NoneRelationFoundError()
        return results

    def clear(self):
        """Clear the cache"""
        with self.lock:
            self.relations.clear()
            self.schemas.clear()

    def _list_relations_in_schema(
        self, database: Optional[str], schema: Optional[str]
    ) -> List[_CachedRelation]:
        """Get the relations in a schema. Callers should hold the lock."""
        key = (lowercase(database), lowercase(schema))

        to_remove: List[_CachedRelation] = []
        for cachekey, relation in self.relations.items():
            if (cachekey.database, cachekey.schema) == key:
                to_remove.append(relation)
        return to_remove

    def _remove_all(self, to_remove: List[_CachedRelation]):
        """Remove all the listed relations. Ignore relations that have been
        cascaded out.
        """
        for relation in to_remove:
            # it may have been cascaded out already
            drop_key = _make_ref_key(relation)
            if drop_key in self.relations:
                self.drop(drop_key)
