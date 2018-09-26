from collections import namedtuple
import threading
from copy import deepcopy
import pprint
from dbt.logger import CACHE_LOGGER as logger
import dbt.exceptions


ReferenceKey = namedtuple('ReferenceKey', 'schema identifier')


def dot_separated(key):
    """Return the key in dot-separated string form.

    :param key ReferenceKey: The key to stringify.
    """
    return '.'.join(key)


class CachedRelation(object):
    """Nothing about CachedRelation is guaranteed to be thread-safe!

    :attr str schema: The schema of this relation.
    :attr str identifier: The identifier of this relation.
    :attr Dict[ReferenceKey, CachedRelation] referenced_by: The relations that
        refer to this relation.
    :attr DefaultRelation inner: The underlying dbt relation.
    """
    def __init__(self, schema, identifier, inner):
        self.schema = schema
        self.identifier = identifier
        self.referenced_by = {}
        self.inner = inner

    def __str__(self):
        return (
            'CachedRelation(schema={}, identifier={}, inner={})'
        ).format(self.schema, self.identifier, self.inner)

    def __copy__(self):
        new = self.__class__(self.schema, self.identifier)
        new.__dict__.update(self.__dict__)
        return new

    def __deepcopy__(self, memo):
        new = self.__class__(self.schema, self.identifier)
        new.__dict__.update(self.__dict__)
        new.referenced_by = deepcopy(self.referenced_by, memo)
        new.inner = self.inner.incorporate()

    def is_referenced_by(self, key):
        return key in self.referenced_by

    def key(self):
        """Get the ReferenceKey that represents this relation

        :return ReferenceKey: A key for this relation.
        """
        return ReferenceKey(self.schema, self.identifier)

    def add_reference(self, referrer):
        """Add a reference from referrer to self, indicating that if this node
        were drop...cascaded, the referrer would be dropped as well.

        :param CachedRelation referrer: The node that refers to this node.
        """
        self.referenced_by[referrer.key()] = referrer

    def collect_consequences(self):
        """Recursively collect a set of ReferenceKeys that would
        consequentially get dropped if this were dropped via
        "drop ... cascade".

        :return Set[ReferenceKey]: All the relations that would be dropped
        """
        consequences = {self.key()}
        for relation in self.referenced_by.values():
            consequences.update(relation.collect_consequences())
        return consequences

    def release_references(self, keys):
        """Non-recursively indicate that an iterable of ReferenceKey no longer
        exist. Unknown keys are ignored.

        :param Iterable[ReferenceKey] keys: The keys to drop.
        """
        keys = set(self.referenced_by) & set(keys)
        for key in keys:
            self.referenced_by.pop(key)

    def rename(self, new_relation):
        """Rename this cached relation to new_relation, updating the inner
        Reference as well.
        Note that this will change the output of key(), all refs must be
        updated!

        :param ReferenceKey new_relation: The new name to apply to the relation
        """
        self.schema = new_relation.schema
        self.identifier = new_relation.identifier
        # rename our inner value as well
        if self.inner:
            # Relations store this stuff inside their `path` dict. But they
            # also store a table_name, and usually use it in their  .render(),
            # so we need to update that as well. It doesn't appear that
            # table_name is ever anything but the identifier (via .create())
            self.inner = self.inner.incorporate(
                path={
                    'schema': new_relation.schema,
                    'identifier': new_relation.identifier
                },
                table_name=new_relation.identifier
            )

    def rename_key(self, old_key, new_key):
        """Rename a reference that may or may not exist. Only handles the
        reference itself, so this is the other half of what `rename` does.

        If old_key is not in referenced_by, this is a no-op.

        :param ReferenceKey old_key: The old key to be renamed.
        :param ReferenceKey new_key: The new key to rename to.
        :raises InternalError: If the new key already exists.
        """
        # we've lost track of the state of the world!
        if new_key in self.referenced_by:
            dbt.exceptions.raise_cache_inconsistent(
                'in rename of "{}" -> "{}", new name is in the cache already'
                .format(old_key, new_key)
            )

        if old_key not in self.referenced_by:
            return
        value = self.referenced_by.pop(old_key)
        self.referenced_by[new_key] = value

    def dump_graph_entry(self):
        """Return a key/value pair representing this key and its referents.

        return List[str]: The dot-separated form of all referent keys.
        """
        return [dot_separated(r) for r in self.referenced_by]


class RelationsCache(object):
    """A cache of the relations known to dbt. Keeps track of relationships
    declared between tables and handles renames/drops as a real database would.

    :attr Dict[ReferenceKey, CachedRelation] relations: The known relations.
    :attr threading.RLock lock: The lock around relations, held during updates.
        The adapters also hold this lock while filling the cache.
    :attr Set[str] schemas: The set of known/cached schemas, all lowercased.
    """
    def __init__(self):
        self.relations = {}
        self.lock = threading.RLock()
        self.schemas = set()

    def add_schema(self, schema):
        """Add a schema to the set of known schemas (case-insensitive)

        :param str schema: The schema name to add.
        """
        self.schemas.add(schema.lower())

    def update_schemas(self, schemas):
        """Add multiple schemas to the set of known schemas (case-insensitive)

        :param Iterable[str] schemas: An iterable of the schema names to add.
        """
        self.schemas.update(s.lower() for s in schemas)

    def __contains__(self, schema):
        """A schema is 'in' the relations cache if it is in the set of cached
        schemas.

        :param str schema: The schema name to look up.
        """
        return schema in self.schemas

    def dump_graph(self):
        """Dump a key-only representation of the schema to a dictionary. Every
        known relation is a key with a value of a list of keys it is referenced
        by.
        """
        # we have to hold the lock while iterating, if other threads modify
        # self.relations during iteration it's a runtime error
        with self.lock:
            # consume the full iterator inside the lock
            items = list(self.relations.items())

        return {dot_separated(k): v.dump_graph_entry() for k, v in items}

    def _setdefault(self, relation):
        """Add a relation to the cache, or return it if it already exists.

        :param CachedRelation relation: The relation to set or get.
        :return CachedRelation: The relation stored under the given relation's
            key
        """
        self.schemas.add(relation.schema)
        key = relation.key()
        return self.relations.setdefault(key, relation)

    def _add_link(self, referenced_key, dependent_key):
        """Add a link between two relations to the database. Both the old and
        new entries must alraedy exist in the database.

        :param ReferenceKey referenced_key: The key identifying the referenced
            model (the one that if dropped will drop the dependent model).
        :param ReferenceKey dependent_key: The key identifying the dependent
            model.
        :raises InternalError: If either entry does not exist.
        """
        # get the canonical referenced entries.
        referenced = self.relations.get(referenced_key)
        if referenced is None:
            dbt.exceptions.raise_cache_inconsistent(
                'in add_link, referenced link key {} not in cache!'
                .format(referenced_key)
            )

        dependent = self.relations.get(dependent_key)
        if dependent is None:
            dbt.exceptions.raise_cache_inconsistent(
                'in add_link, dependent link key {} not in cache!'
                .format(dependent_key)
            )

        # link them up
        referenced.add_reference(dependent)

    def add_link(self, referenced_schema, referenced_name, dependent_schema,
                 dependent_name):
        """Add a link between two relations to the database. Both the old and
        new entries must already exist in the database.

        The dependent model refers _to_ the referenced model. So, given
        arguments of (jake_test, bar, jake_test, foo):
        both values are in the schema jake_test and foo is a view that refers
        to bar, so "drop bar cascade" will drop foo and all of foo's
        dependents.

        :param str referenced_schema: The schema of the referenced model.
        :param str referenced_name: The identifier of the referenced model.
        :param str dependent_schema: The schema of the dependent model.
        :param str dependent_name: The identifier of the dependent model.
        :raises InternalError: If either entry does not exist.
        """
        referenced = ReferenceKey(
            schema=referenced_schema,
            identifier=referenced_name
        )
        dependent = ReferenceKey(
            schema=dependent_schema,
            identifier=dependent_name
        )
        logger.debug(
            'adding link, {!s} references {!s}'.format(dependent, referenced)
        )
        logger.debug('before adding link: {}'.format(
            pprint.pformat(self.dump_graph()))
        )
        with self.lock:
            self._add_link(referenced, dependent)
        logger.debug('after adding link: {}'.format(
            pprint.pformat(self.dump_graph()))
        )

    def add(self, schema, identifier, inner):
        """Add the relation inner to the cache, under the schema schema and
        identifier identifier

        :param str schema: The schema.
        :param str identifier: The identifier.
        :param DefaultRelation inner: The underlying relation.
        """
        relation = CachedRelation(
            schema=schema,
            identifier=identifier,
            inner=inner
        )
        logger.debug('Adding relation: {!s}'.format(relation))
        logger.debug('before adding: {}'.format(
            pprint.pformat(self.dump_graph()))
        )
        with self.lock:
            self._setdefault(relation)
        logger.debug('after adding: {}'.format(
            pprint.pformat(self.dump_graph()))
        )

    def _remove_refs(self, keys):
        """Removes all references to all entries in keys. This does not
        cascade!

        :param Iterable[ReferenceKey] keys: The keys to remove.
        """
        # remove direct refs
        for key in keys:
            del self.relations[key]
        # then remove all entries from each child
        for cached in self.relations.values():
            cached.release_references(keys)

    def _drop_cascade_relation(self, dropped):
        """Drop the given relation and cascade it appropriately to all
        dependent relations.

        :param CachedRelation dropped: An existing CachedRelation to drop.
        """
        if dropped not in self.relations:
            # dbt drops potentially non-existent relations all the time, so
            # this is fine.
            logger.debug('dropped a nonexistent relationship: {!s}'
                         .format(dropped))
            return
        consequences = self.relations[dropped].collect_consequences()
        logger.debug(
            'drop {} is cascading to {}'.format(dropped, consequences)
        )
        self._remove_refs(consequences)

    def drop(self, schema, identifier):
        """Drop the named relation and cascade it appropriately to all
        dependent relations.

        Because dbt proactively does many `drop relation if exist ... cascade`
        that are noops, nonexistent relation drops cause a debug log and no
        other actions.

        :param str schema: The schema of the relation to drop.
        :param str identifier: The identifier of the relation to drop.
        """
        dropped = ReferenceKey(schema=schema, identifier=identifier)
        logger.debug('Dropping relation: {!s}'.format(dropped))
        logger.debug('before drop: {}'.format(
            pprint.pformat(self.dump_graph()))
        )
        with self.lock:
            self._drop_cascade_relation(dropped)
        logger.debug('after drop: {}'.format(
            pprint.pformat(self.dump_graph()))
        )

    def _rename_relation(self, old_key, new_key):
        """Rename a relation named old_key to new_key, updating references.
        If the new key is already present, that is an error.
        If the old key is absent, we only debug log and return, assuming it's a
        temp table being renamed.

        :param ReferenceKey old_key: The existing key, to rename from.
        :param ReferenceKey new_key: The new key, to rename to.
        :raises InternalError: If the new key is already present.
        """
        # the old relation might not exist. In that case, dbt created this
        # relation earlier in its run and we can ignore it, as we don't care
        # about the rename either
        if old_key not in self.relations:
            logger.debug(
                'old key {} not found in self.relations, assuming temporary'
                .format(old_key)
            )
            return
        # not good
        if new_key in self.relations:
            dbt.exceptions.raise_cache_inconsistent(
                'in rename, new key {} already in cache: {}'
                .format(new_key, list(self.relations.keys()))
            )

        # On the database level, a rename updates all values that were
        # previously referenced by old_name to be referenced by new_name.
        # basically, the name changes but some underlying ID moves. Kind of
        # like an object reference!
        # Get the canonical version of old_relation and remove it from the db
        relation = self.relations.pop(old_key)

        # change the old_relation's name and schema to the new relation's
        relation.rename(new_key)
        # update all the relations that refer to it
        for cached in self.relations.values():
            if cached.is_referenced_by(old_key):
                logger.debug(
                    'updated reference from {0} -> {2} to {1} -> {2}'
                    .format(old_key, new_key, cached.key())
                )
                cached.rename_key(old_key, new_key)

        self.relations[new_key] = relation

    def rename(self, old_schema, old_identifier, new_schema,
               new_identifier):
        """Rename the old schema/identifier to the new schema/identifier and
        update references.

        If the new schema/identifier is already present, that is an error.
        If the schema/identifier key is absent, we only debug log and return,
        assuming it's a temp table being renamed.

        :param str old_schema: The existing schema name.
        :param str old_identifier: The existing identifier name.
        :param str new_schema: The new schema name.
        :param str new_identifier: The new identifier name.
        :raises InternalError: If the new key is already present.
        """
        old_key = ReferenceKey(
            schema=old_schema,
            identifier=old_identifier
        )
        new_key = ReferenceKey(
            schema=new_schema,
            identifier=new_identifier
        )
        logger.debug('Renaming relation {!s} to {!s}'.format(
            old_key, new_key)
        )
        logger.debug('before rename: {}'.format(
            pprint.pformat(self.dump_graph()))
        )
        with self.lock:
            self._rename_relation(old_key, new_key)
        logger.debug('after rename: {}'.format(
            pprint.pformat(self.dump_graph()))
        )

    def _get_cache_value(self, schema, identifier):
        """Get the underlying cache value. Raises a KeyError if it does not
        exist. This is intended for unit testing, mostly.

        :param str schema: The (case-sensitive!) schema name to look for.
        :param str identifier: The identifier to look for.
        :return DefaultRelation: The matching relation.
        :raises KeyError: If the referenced value does not exist.
        """
        key = ReferenceKey(schema=schema, identifier=identifier)
        return self.relations[key]

    def get_relations(self, schema):
        """Case-insensitively yield all relations matching the given schema.

        :param str schema: The case-insensitive schema name to list from.
        :return List[DefaultRelation]: The list of relations with the given
            schema
        """
        schema = schema.lower()
        with self.lock:
            results = [
                r.inner for r in self.relations.values()
                if r.schema.lower() == schema
            ]

        if None in results:
            dbt.exceptions.raise_cache_inconsistent(
                'in get_relations, a None relation was found in the cache!'
            )
        return results

    def clear(self):
        """Clear the cache"""
        with self.lock:
            self.relations.clear()
            self.schemas.clear()
