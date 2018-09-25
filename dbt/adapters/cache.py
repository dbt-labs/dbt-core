from collections import namedtuple
import threading
from copy import deepcopy
import pprint
from dbt.logger import CACHE_LOGGER as logger
import dbt.exceptions

ReferenceKey = namedtuple('ReferenceKey', 'schema identifier')


class CachedRelation(object):
    # TODO: should this more directly related to the Relation class in the
    # adapters themselves?
    """Nothing about CachedRelation is guaranteed to be thread-safe!"""
    def __init__(self, schema, identifier, inner):
        self.schema = schema
        self.identifier = identifier
        self.referenced_by = {}
        # the underlying Relation
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

    def key(self):
        return ReferenceKey(self.schema, self.identifier)

    def add_reference(self, referrer):
        self.referenced_by[referrer.key()] = referrer

    def collect_consequences(self):
        """Recursively collect a set of ReferenceKeys that would
        consequentially get dropped if this were dropped via
        "drop ... cascade".
        """
        consequences = {self.key()}
        for relation in self.referenced_by.values():
            consequences.update(relation.collect_consequences())
        return consequences

    def release_references(self, keys):
        """Non-recursively indicate that an iterable of ReferenceKey no longer
        exist. Unknown keys are ignored.
        """
        keys = set(self.referenced_by) & set(keys)
        for key in keys:
            self.referenced_by.pop(key)

    def rename(self, new_relation):
        """Note that this will change the output of key(), all refs must be
        updated!
        """
        self.schema = new_relation.schema
        self.identifier = new_relation.identifier
        # rename our inner value as well
        if self.inner:
            # Relations store this stuff inside their `path` dict.
            # but they also store a table_name, and conditionally use it in
            # .render(), so we need to update that as well...
            # TODO: is this an aliasing issue? Do I have to worry about this?
            self.inner = self.inner.incorporate(
                path={
                    'schema': new_relation.schema,
                    'identifier': new_relation.identifier
                },
                table_name=new_relation.identifier
            )

    def rename_key(self, old_key, new_key):
            # we've lost track of the state of the world!
        assert new_key not in self.referenced_by, \
            'Internal consistency error: new name is in the cache already'
        if old_key not in self.referenced_by:
            return
        value = self.referenced_by.pop(old_key)
        self.referenced_by[new_key] = value


class RelationsCache(object):
    def __init__(self):
        # map (schema, name) -> CachedRelation object.
        # I think we can ignore database/project?
        self.relations = {}
        # make this a reentrant lock so the adatper can hold it while buliding
        # the cache.
        self.lock = threading.RLock()
        # the set of cached schemas
        self.schemas = set()

    def dump_graph(self):
        return {
            '{}.{}'.format(k.schema, k.identifier):
                [
                    '{}.{}'.format(x.schema, x.identifier)
                    for x in v.referenced_by
                ]
            for k, v in self.relations.items()
        }

    def _setdefault(self, relation):
        self.schemas.add(relation.schema)
        key = relation.key()
        return self.relations.setdefault(key, relation)

    def _add_link(self, referenced_key, dependent_key):
        # get the canonical referenced entries. both entries must exist!
        referenced = self.relations.get(referenced_key)
        if referenced is None:
            dbt.exceptions.raise_cache_inconsistent(
                'link key {} not in cache!'.format(referenced_key)
            )

        dependent = self.relations.get(dependent_key)
        if dependent is None:
            dbt.exceptions.raise_cache_inconsistent(
                'link key {} not in cache!'.format(dependent_key)
            )

        # link them up
        referenced.add_reference(dependent)

    def add_link(self, referenced_schema, referenced_name, dependent_schema,
                 dependent_name):
        """The dependent schema refers _to_ the referenced schema

        # given arguments of:
        # (jake_test, bar, jake_test, foo, view)
        # foo is a view that refers to bar -> "drop bar cascade" will drop foo
        # and all of foo's dependencies, recursively
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
        # remove direct refs
        for key in keys:
            del self.relations[key]
        # then remove all entries from each child
        for cached in self.relations.values():
            cached.release_references(keys)

    def _drop_cascade_relation(self, dropped):
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
                '{} in {}'.format(new_key, list(self.relations.keys()))
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
            if old_key in cached.referenced_by:
                logger.debug(
                    'updated reference from {0} -> {2} to {1} -> {2}'
                    .format(old_key, new_key, cached.key())
                )
                cached.rename_key(old_key, new_key)

        self.relations[new_key] = relation

    def rename_relation(self, old_schema, old_identifier, new_schema,
                        new_identifier):
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

    def _get_relation(self, schema, identifier):
        """Get the relation by name. Raises a KeyError if it does not exist"""
        key = ReferenceKey(schema=schema, identifier=identifier)
        return self.relations[key]

    def get_relations(self, schema):
        """Case-insensitively yield all relations matching the given schema.
        """
        # TODO: What do we do if the inner value is None? Should that be
        # possible?
        schema = schema.lower()
        with self.lock:
            results = [
                r.inner for r in self.relations.values()
                if r.schema.lower() == schema
            ]
        if None in results:
            dbt.exceptions.raise_cache_inconsistent(
                'A None relation was found in the cache!'
            )
        return results

    def clear(self):
        with self.lock:
            self.relations.clear()
            self.schemas.clear()
