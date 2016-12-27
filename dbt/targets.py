
import psycopg2
import os
import logging

THREAD_MIN = 1
THREAD_MAX = 8

BAD_THREADS_ERROR = """Invalid value given for "threads" in active target.
Value given was {supplied} but it should be an int between {min_val} and
{max_val}."""


class BaseSQLTarget(object):
    def __init__(self, cfg, threads):
        self.target_type = cfg['type']
        self.host = cfg['host']
        self.user = cfg['user']
        self.password = cfg['pass']
        self.port = cfg['port']
        self.dbname = cfg['dbname']
        self.schema = cfg['schema']

        self.threads = self.__get_threads(cfg, threads)

        self.ssh_host = None
        self.handle = None

    def should_open_tunnel(self):
        return False

    # make the user explicitly call this function to enable the ssh tunnel
    # we don't want it to be automatically opened any time someone makes a new
    # target
    def open_tunnel_if_needed(self):
        pass

    def cleanup(self):
        pass

    def __get_threads(self, cfg, cli_threads=None):
        if cli_threads is None:
            supplied = cfg.get('threads', 1)
        else:
            supplied = cli_threads

        bad_threads_error = RuntimeError(
            BAD_THREADS_ERROR.format(
                supplied=supplied,
                min_val=THREAD_MIN,
                max_val=THREAD_MAX
            )
        )

        if type(supplied) != int:
            raise bad_threads_error

        if supplied >= THREAD_MIN and supplied <= THREAD_MAX:
            return supplied
        else:
            raise bad_threads_error

    def __get_spec(self):
        return "dbname='{}' user='{}' host='{}' password='{}' port='{}' " \
            "connect_timeout=10".format(
                self.dbname,
                self.user,
                self.host,
                self.password,
                self.port
            )

    def get_handle(self):
        # this is important -- if we use different handles, then redshift
        # fails with a message about concurrent transactions
        if self.handle is None:
            self.handle = psycopg2.connect(self.__get_spec())
        return self.handle

    def rollback(self):
        if self.handle is not None:
            self.handle.rollback()

    @property
    def type(self):
        return self.target_type


class RedshiftTarget(BaseSQLTarget):
    def __init__(self, cfg, threads):
        super(RedshiftTarget, self).__init__(cfg, threads)

    @property
    def context(self):
        return {
            "sql_now": "getdate()"
        }

    def sort_qualifier(self, sort_type, sort_keys):

        valid_sort_types = ['compound', 'interleaved']
        if sort_type not in valid_sort_types:
            raise RuntimeError(
                "Invalid sort_type given: {} -- must be one of {}"
                .format(sort_type, valid_sort_types)
            )

        if type(sort_keys) == str:
            sort_keys = [sort_keys]

        formatted_sort_keys = ['"{}"'.format(sort_key)
                               for sort_key in sort_keys]
        keys_csv = ', '.join(formatted_sort_keys)

        return "{sort_type} sortkey({keys_csv})".format(
            sort_type=sort_type, keys_csv=keys_csv
        )

    def dist_qualifier(self, dist_key):
        dist_key = dist_key.strip().lower()

        if dist_key in ['all', 'even']:
            return 'diststyle({})'.format(dist_key)
        else:
            return 'diststyle key distkey("{}")'.format(dist_key)


class PostgresTarget(BaseSQLTarget):
    def __init__(self, cfg, threads):
        super(PostgresTarget, self).__init__(cfg, threads)

    def dist_qualifier(self, dist_key):
        return ''

    def sort_qualifier(self, sort_type, sort_keys):
        return ''

    @property
    def context(self):
        return {
            "sql_now": "clock_timestamp()"
        }

target_map = {
    'postgres': PostgresTarget,
    'redshift': RedshiftTarget
}


def get_target(cfg, threads=1):
    target_type = cfg['type']
    if target_type in target_map:
        klass = target_map[target_type]
        return klass(cfg, threads)
    else:
        valid_csv = ", ".join(["'{}'".format(t) for t in target_map])
        raise RuntimeError(
            "Invalid target type provided: '{}'. Must be one of {}"
            .format(target_type, valid_csv)
        )
