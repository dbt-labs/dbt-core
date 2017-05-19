import multiprocessing

from dbt.adapters.postgres import PostgresAdapter
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
from dbt.compat import basestring


drop_lock = multiprocessing.Lock()


class RedshiftAdapter(PostgresAdapter):

    @classmethod
    def type(cls):
        return 'redshift'

    @classmethod
    def date_function(cls):
        return 'getdate()'

    @classmethod
    def dist_qualifier(cls, dist):
        dist_key = dist.strip().lower()

        if dist_key in ['all', 'even']:
            return 'diststyle {}'.format(dist_key)
        else:
            return 'diststyle key distkey("{}")'.format(dist_key)

    @classmethod
    def sort_qualifier(cls, sort_type, sort):
        valid_sort_types = ['compound', 'interleaved']
        if sort_type not in valid_sort_types:
            raise RuntimeError(
                "Invalid sort_type given: {} -- must be one of {}"
                .format(sort_type, valid_sort_types)
            )

        if isinstance(sort, basestring):
            sort_keys = [sort]
        else:
            sort_keys = sort

        formatted_sort_keys = ['"{}"'.format(sort_key)
                               for sort_key in sort_keys]
        keys_csv = ', '.join(formatted_sort_keys)

        return "{sort_type} sortkey({keys_csv})".format(
            sort_type=sort_type, keys_csv=keys_csv
        )

    @classmethod
    def drop(cls, profile, relation, relation_type, model_name=None):
        global drop_lock

        to_return = None

        try:
            drop_lock.acquire()

            connection = cls.get_connection(profile, model_name)

            if connection.get('transaction_open'):
                cls.commit(connection)

            cls.begin(profile, connection.get('name'))

            to_return = super(PostgresAdapter, cls).drop(
                profile, relation, relation_type, model_name)

            cls.commit(connection)
            cls.begin(profile, connection.get('name'))

            return to_return

        finally:
            drop_lock.release()
