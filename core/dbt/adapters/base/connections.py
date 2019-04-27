import abc
import multiprocessing

import six

import dbt.exceptions
import dbt.flags
from dbt.api import APIObject
from dbt.compat import abstractclassmethod
from dbt.contracts.connection import Connection
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import translate_aliases


class Credentials(APIObject):
    """Common base class for credentials. This is not valid to instantiate"""
    SCHEMA = NotImplemented
    # map credential aliases to their canonical names.
    ALIASES = {}

    def __init__(self, **kwargs):
        renamed = self.translate_aliases(kwargs)
        super(Credentials, self).__init__(**renamed)

    @property
    def type(self):
        raise NotImplementedError(
            'type not implemented for base credentials class'
        )

    def connection_info(self):
        """Return an ordered iterator of key/value pairs for pretty-printing.
        """
        for key in self._connection_keys():
            if key in self._contents:
                yield key, self._contents[key]

    def _connection_keys(self):
        """The credential object keys that should be printed to users in
        'dbt debug' output. This is specific to each adapter.
        """
        raise NotImplementedError

    def incorporate(self, **kwargs):
        # implementation note: we have to do this here, or
        # incorporate(alias_name=...) will result in duplicate keys in the
        # merged dict that APIObject.incorporate() creates.
        renamed = self.translate_aliases(kwargs)
        return super(Credentials, self).incorporate(**renamed)

    def serialize(self, with_aliases=False):
        serialized = super(Credentials, self).serialize()
        if with_aliases:
            serialized.update({
                new_name: serialized[canonical_name]
                for new_name, canonical_name in self.ALIASES.items()
                if canonical_name in serialized
            })
        return serialized

    @classmethod
    def translate_aliases(cls, kwargs):
        return translate_aliases(kwargs, cls.ALIASES)


@six.add_metaclass(abc.ABCMeta)
class BaseConnectionManager(object):
    """Methods to implement:
        - exception_handler
        - cancel_open
        - open
        - begin
        - commit
        - execute

    You must also set the 'TYPE' class attribute with a class-unique constant
    string.
    """
    TYPE = NotImplemented

    def __init__(self, profile):
        self.profile = profile
        self.in_use = {}
        self.available = []
        self.lock = multiprocessing.RLock()
        self._set_initial_connections()

    def _set_initial_connections(self):
        self.available = []
        # set up the array of connections in the 'init' state.
        # we add a magic number, 2 because there are overhead connections,
        # one for pre- and post-run hooks and other misc operations that occur
        # before the run starts, and one for integration tests.
        for idx in range(self.profile.threads + 2):
            self.available.append(self._empty_connection())

    def _empty_connection(self):
        return Connection(
            type=self.TYPE,
            name=None,
            state='init',
            transaction_open=False,
            handle=None,
            credentials=self.profile.credentials
        )

    @abc.abstractmethod
    def exception_handler(self, sql, connection_name='master'):
        """Create a context manager that handles exceptions caused by database
        interactions.

        :param str sql: The SQL string that the block inside the context
            manager is executing.
        :param str connection_name: The name of the connection being used
        :return: A context manager that handles exceptions raised by the
            underlying database.
        """
        raise dbt.exceptions.NotImplementedException(
            '`exception_handler` is not implemented for this adapter!')

    def get(self, name=None):
        """This is thread-safe as long as two threads don't use the same
        "name".
        """
        if name is None:
            # if a name isn't specified, we'll re-use a single handle
            # named 'master'
            name = 'master'

        with self.lock:
            if name in self.in_use:
                return self.in_use[name]

            logger.debug('Acquiring new {} connection "{}".'
                         .format(self.TYPE, name))

            if not self.available:
                raise dbt.exceptions.InternalException(
                    'Tried to request a new connection "{}" but '
                    'the maximum number of connections are already '
                    'allocated!'.format(name)
                )

            connection = self.available.pop()
            # connection is temporarily neither in use nor available, but both
            # collections are in a sane state, so we can release the lock.

        # this potentially calls open(), but does so without holding the lock
        connection = self.assign(connection, name)

        with self.lock:
            if name in self.in_use:
                raise dbt.exceptions.InternalException(
                    'Two threads concurrently tried to get the same name: {}'
                    .format(name)
                )
            self.in_use[name] = connection

        return connection

    @abc.abstractmethod
    def cancel_open(self):
        """Cancel all open connections on the adapter. (passable)"""
        raise dbt.exceptions.NotImplementedException(
            '`cancel_open` is not implemented for this adapter!'
        )

    @abstractclassmethod
    def open(cls, connection):
        """Open a connection on the adapter.

        This may mutate the given connection (in particular, its state and its
        handle).

        This should be thread-safe, or hold the lock if necessary. The given
        connection should not be in either in_use or available.

        :param Connection connection: A connection object to open.
        :return: A connection with a handle attached and an 'open' state.
        :rtype: Connection
        """
        raise dbt.exceptions.NotImplementedException(
            '`open` is not implemented for this adapter!'
        )

    def assign(self, conn, name):
        """Open a connection if it's not already open, and assign it name
        regardless.

        The caller is responsible for putting the assigned connection into the
        in_use collection.

        :param Connection conn: A connection, in any state.
        :param str name: The name of the connection to set.
        """
        if name is None:
            name = 'master'

        conn.name = name

        if conn.state == 'open':
            logger.debug('Re-using an available connection from the pool.')
        else:
            logger.debug('Opening a new connection, currently in state {}'
                         .format(conn.state))
            conn = self.open(conn)

        return conn

    def _release_connection(self, conn):
        if conn.state == 'open':
            if conn.transaction_open is True:
                self._rollback(conn)
            conn.name = None
        else:
            self.close(conn)

    def release(self, name):
        with self.lock:
            if name not in self.in_use:
                return

            to_release = self.in_use.pop(name)
            # to_release is temporarily neither in use nor available, but both
            # collections are in a sane state, so we can release the lock.

        try:
            self._release_connection(to_release)
        except:
            # if rollback or close failed, replace our busted connection with
            # a new one
            to_release = self._empty_connection()
            raise
        finally:
            # now that this connection has been rolled back and the name reset,
            # or the connection has been closed, put it back on the available
            # list
            with self.lock:
                self.available.append(to_release)

    def cleanup_all(self):
        with self.lock:
            for name, connection in self.in_use.items():
                if connection.state != 'closed':
                    logger.debug("Connection '{}' was left open."
                                 .format(name))
                else:
                    logger.debug("Connection '{}' was properly closed."
                                 .format(name))

            conns_in_use = list(self.in_use.values())
            for conn in conns_in_use + self.available:
                self.close(conn)

            # garbage collect these connections
            self.in_use.clear()
            self._set_initial_connections()

    @abc.abstractmethod
    def begin(self, name):
        """Begin a transaction. (passable)

        :param str name: The name of the connection to use.
        """
        raise dbt.exceptions.NotImplementedException(
            '`begin` is not implemented for this adapter!'
        )

    def get_if_exists(self, name):
        if name is None:
            name = 'master'

        if self.in_use.get(name) is None:
            return

        return self.get(name)

    @abc.abstractmethod
    def commit(self, connection):
        """Commit a transaction. (passable)

        :param str name: The name of the connection to use.
        """
        raise dbt.exceptions.NotImplementedException(
            '`commit` is not implemented for this adapter!'
        )

    def _rollback_handle(self, connection):
        """Perform the actual rollback operation."""
        connection.handle.rollback()

    def _rollback(self, connection):
        """Roll back the given connection.

        The connection does not have to be in in_use or available, so this
        operation does not require the lock.
        """
        if dbt.flags.STRICT_MODE:
            assert isinstance(connection, Connection)

        if connection.transaction_open is False:
            raise dbt.exceptions.InternalException(
                'Tried to rollback transaction on connection "{}", but '
                'it does not have one open!'.format(connection.name))

        logger.debug('On {}: ROLLBACK'.format(connection.name))
        self._rollback_handle(connection)

        connection.transaction_open = False

        return connection

    @classmethod
    def close(cls, connection):
        if dbt.flags.STRICT_MODE:
            assert isinstance(connection, Connection)

        # if the connection is in closed or init, there's nothing to do
        if connection.state in {'closed', 'init'}:
            return connection

        if connection.transaction_open and connection.handle:
            connection.handle.rollback()
        connection.transaction_open = False

        # On windows, sometimes connection handles don't have a close() attr.
        if hasattr(connection.handle, 'close'):
            connection.handle.close()
        else:
            logger.debug('On {}: No close available on handle'
                         .format(connection.name))

        connection.state = 'closed'

        return connection

    def commit_if_has_connection(self, name):
        """If the named connection exists, commit the current transaction.

        :param str name: The name of the connection to use.
        """
        connection = self.in_use.get(name)
        if connection:
            self.commit(connection)

    def clear_transaction(self, conn_name='master'):
        conn = self.begin(conn_name)
        self.commit(conn)
        return conn_name

    @abc.abstractmethod
    def execute(self, sql, name=None, auto_begin=False, fetch=False):
        """Execute the given SQL.

        :param str sql: The sql to execute.
        :param Optional[str] name: The name to use for the connection.
        :param bool auto_begin: If set, and dbt is not currently inside a
            transaction, automatically begin one.
        :param bool fetch: If set, fetch results.
        :return: A tuple of the status and the results (empty if fetch=False).
        :rtype: Tuple[str, agate.Table]
        """
        raise dbt.exceptions.NotImplementedException(
            '`execute` is not implemented for this adapter!'
        )
