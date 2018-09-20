from collections import namedtuple
import threading
from dbt.logger import GLOBAL_LOGGER as logger
from copy import deepcopy

ReferenceKey = namedtuple('ReferenceKey', 'schema identifier')


class CachedRelation(object):
    # TODO: should this more directly related to the Relation class in the
    # adapters themselves?
    """Nothing about CachedRelation is guaranteed to be thread-safe!"""
    def __init__(self, schema, identifier, kind=None, inner=None):
        self.schema = schema
        self.identifier = identifier
        # This might be None, if the table is only referenced _by_ things, or
        # temporariliy during cache building
        # TODO: I'm still not sure we need this
        self.kind = kind
        self.referenced_by = {}
        # a inner to store on this cached relation.
        self.inner = inner

    def __str__(self):
        return (
            'CachedRelation(schema={}, identifier={}, kind={}, inner={})'
        ).format(self.schema, self.identifier, self.kind, self.inner)

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
                table_name = new_relation.identifier
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

    def _setdefault(self, relation):
        self.schemas.add(relation.schema)
        key = relation.key()
        result = self.relations.setdefault(key, relation)
        # if we previously only saw the dependent without any kind information,
        # update the type info.
        if relation.kind is not None:
            if result.kind is None:
                result.kind = relation.kind
            # we've lost track of the state of the world!
            assert result.kind == relation.kind, \
                'Internal consistency error: Different non-None relation kinds'
        # ditto for inner, except overwriting is fine
        if relation.inner is not None:
            if result.inner is None:
                result.inner = relation.inner
        return result

    def _add_link(self, new_referenced, new_dependent):
        # get the canonical referenced entries (our new one could be canonical)
        referenced = self._setdefault(new_referenced)
        dependent = self._setdefault(new_dependent)

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
        referenced = CachedRelation(
            schema=referenced_schema,
            identifier=referenced_name
        )
        dependent = CachedRelation(
            schema=dependent_schema,
            identifier=dependent_name
        )
        logger.debug('adding link, {!s} references {!s}'
            .format(dependent, referenced)
        )
        with self.lock:
            self._add_link(referenced, dependent)

    def add(self, schema, identifier, kind=None, inner=None):
        relation = CachedRelation(
            schema=schema,
            identifier=identifier,
            kind=kind,
            inner=inner
        )
        logger.debug('Adding relation: {!s}'.format(relation))
        with self.lock:
            self._setdefault(relation)

    def _remove_refs(self, keys):
        # remove direct refs
        for key in keys:
            del self.relations[key]
        # then remove all entries from each child
        for cached in self.relations.values():
            cached.release_references(keys)

    def _drop_cascade_relation(self, dropped):
        key = dropped.key()
        if key not in self.relations:
            # dbt drops potentially non-existent relations all the time, so
            # this is fine.
            logger.debug('dropped a nonexistent relationship: {!s}'
                         .format(dropped.key()))
            return
        consequences = self.relations[key].collect_consequences()
        logger.debug('drop {} is cascading to {}'.format(key, consequences))
        self._remove_refs(consequences)

    def drop(self, schema, identifier):
        dropped = CachedRelation(schema=schema, identifier=identifier)
        logger.debug('Dropping relation: {!s}'.format(dropped))
        with self.lock:
            self._drop_cascade_relation(dropped)

    def _rename_relation(self, old_relation, new_relation):
        old_key = old_relation.key()
        new_key = new_relation.key()
        # the old relation might not exist. In that case, dbt created this
        # relation earlier in its run and we can ignore it, as we don't care
        # about the rename either
        if old_key not in self.relations:
            return
        # not good
        if new_key in self.relations:
            raise RuntimeError(
                'Internal consistency error!: {} in {}'
                .format(new_key, list(self.relations.keys()))
            )

        # On the database level, a rename updates all values that were
        # previously referenced by old_name to be referenced by new_name.
        # basically, the name changes but some underlying ID moves. Kind of
        # like an object reference!
        # Get the canonical version of old_relation and remove it from the db
        relation = self.relations.pop(old_key)

        # change the old_relation's name and schema to the new relation's
        relation.rename(new_relation)
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
        old_relation = CachedRelation(
            schema=old_schema,
            identifier=old_identifier
        )
        new_relation = CachedRelation(
            schema=new_schema,
            identifier=new_identifier
        )
        logger.debug('Renaming relation {!s} to {!s}'.format(
            old_relation, new_relation)
        )
        with self.lock:
            self._rename_relation(old_relation, new_relation)

    def _get_relation(self, schema, identifier):
        """Get the relation by name. Raises a KeyError if it does not exist"""
        relation = CachedRelation(schema=schema, identifier=identifier)
        return self.relations[relation.key()]

    def get_relations(self, schema):
        """Case-insensitively yield all relations matching the given schema.
        """
        # TODO: What do we do if the inner value is None? Should that be
        # possible?
        schema = schema.lower()
        with self.lock:
            return [
                r.inner for r in self.relations.values()
                if r.schema.lower() == schema
            ]
