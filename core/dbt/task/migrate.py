import itertools
import os

from dbt.task.base import BaseTask

from dbt.config import RuntimeConfig
from dbt.adapters.factory import get_adapter
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.clients import system
from dbt.exceptions import RuntimeException


_UNQUOTE_RENAME_COLUMNS = (
    ('valid_from', 'dbt_valid_from'),
    ('valid_to', 'dbt_valid_to'),
    ('scd_id', 'dbt_scd_id'),
)


SNAPSHOT_TEMPLATE = '''
{{% snapshot {name} %}}
    {{{{
        config({kwargs})
    }}}}
    select * from {source_relation}
{{% endsnapshot %}}
'''


class Migrator:
    """Migrate a single archive config"""
    def __init__(self, manager, adapter, archive_def):
        self.manager = manager
        self.archive_def = archive_def
        self.adapter = adapter
        self.relation = adapter.Relation.create(
            database=self.archive_def['target_database'],
            schema=self.archive_def['target_schema'],
            identifier=self.archive_def['target_table'],
            quote_policy=self.manager.config.quoting
        )
        self.backup = self.append_relation_name()
        self.snapshot_path = os.path.join(self.manager.snapshot_root,
                                          self.relation.identifier + '.sql')

    def get_renamed_columns(self, quote=True):
        columns = []

        unquote = _UNQUOTE_RENAME_COLUMNS
        if self.adapter.type() == 'snowflake':
            unquote = itertools.chain(
                _UNQUOTE_RENAME_COLUMNS,
                [('dbt_updated_at', 'dbt_updated_at')]
            )

        for old, new in unquote:
            if quote:
                old = self.adapter.quote(old)
            columns.append((old, new))
        return columns

    def append_relation_name(self, end='_dbt_archive_migration_backup'):
        new_name = self.relation.identifier + end
        backup = self.relation.incorporate(
            path={'identifier': new_name}, table_name=new_name
        )
        return backup

    def migrate_archive_ctas(self, dest):
        # get the columns
        columns = self.adapter.get_columns_in_relation(self.relation)
        if len(columns) == 0:
            # the archive target must not exist? Continue, that is ok.
            logger.info('  - Table {} does not exist, nothing to migrate.'
                        .format(self.relation))
            return
        cols = {c.name.lower(): c.name for c in columns}
        renames = self.get_renamed_columns()
        select_parts = []
        select_as_parts = []
        for old, new in renames:
            key = old.strip('"').lower()
            if key not in cols:
                raise Exception(
                    'expected column like {} not but it is not in the table!'
                    .format(key)
                )
            del cols[key]
            select_as_parts.append('{} as {}'.format(old, new))

        for column in columns:
            name = column.name
            if name.lower() in cols:
                select_parts.append(self.adapter.quote(name))

        selections = ', '.join(itertools.chain(select_parts, select_as_parts))
        ctas = 'create table {!s} as (select {} from {!s})'.format(
            dest, selections, self.relation
        )
        self.adapter.execute(ctas)

    def migrate_archive_postgres(self):
        """Migrate the archive using "alter table" commands to rename columns.
        """
        self.adapter.connections.add_begin_query()
        tmp = self.append_relation_name('_dbt_archive_migration_tmp')
        logger.debug('  - Making new archive at {}'.format(tmp))
        self.migrate_archive_ctas(dest=tmp)
        logger.info('  - Backing up table to {}'.format(self.backup))
        self.adapter.rename_relation(self.relation, self.backup)
        logger.debug('  - Renaming temp archive to final')
        self.adapter.rename_relation(tmp, self.relation)
        self.adapter.connections.add_commit_query()

    def migrate_archive_snowflake(self):
        """Migrate the archive by create table as select ..."""
        logger.debug('  - Making new archive at {}'.format(self.backup))
        self.migrate_archive_ctas(dest=self.backup)
        logger.info('  - Backing up table to {}'.format(self.backup))
        self.adapter.execute('alter table {!s} swap with {!s}'.format(
            self.relation, self.backup
        ))

    def migrate_archive_bigquery(self):
        """Migrate the archive by select * EXCEPT(...) into itself
        """
        logger.info('  - Backing up table to {}'.format(self.backup))
        self.adapter.execute(
            'create table {} as (select * from {})'
            .format(self.backup, self.relation)
        )
        columns = self.get_renamed_columns()
        except_str = ', '.join(o for o, _ in columns)
        rename_str = ', '.join('{} as {}'.format(o, n) for o, n in columns)
        sql = 'select * EXCEPT({}), {} from {!s}'.format(
            except_str, rename_str, self.relation
        )
        self.adapter.connections.create_table(
            database=self.relation.database,
            schema=self.relation.schema,
            table_name=self.relation.identifier,
            sql=sql
        )

    def migrate_archive_table(self):
        logger.info('  - Starting table migration')

        if self.adapter.type() == 'bigquery':
            self.migrate_archive_bigquery()
        elif self.adapter.type() == 'snowflake':
            self.migrate_archive_snowflake()
        else:
            self.migrate_archive_postgres()
        logger.info('  - Finished table migration')

    def build_archive_data(self):
        source_relation = [
            self.adapter.quote_as_configured(
                self.archive_def['source_database'], 'database'
            ),
            self.adapter.quote_as_configured(
                self.archive_def['source_schema'],
                'schema'
            ),
            self.adapter.quote_as_configured(
                self.archive_def['source_table'],
                'identifier'
            ),
        ]

        kwargs = {
            'target_database': self.archive_def['target_database'],
            'target_schema': self.archive_def['target_schema'],
            'updated_at': self.archive_def['updated_at'],
            'strategy': 'timestamp',
            'unique_key': self.archive_def['unique_key'],
        }

        return SNAPSHOT_TEMPLATE.format(
            source_relation='.'.join(source_relation),
            kwargs=repr(kwargs),
            name=self.archive_def['target_table']
        )

    def write_file(self):
        logger.debug('  - Writing snapshot to {}'.format(self.snapshot_path))
        contents = self.build_archive_data()

        wrote = system.make_file(path=self.snapshot_path, contents=contents)
        if wrote:
            logger.info('  - Wrote new snapshot file to {}'
                        .format(self.snapshot_path))
        else:
            logger.error('  - Error: Could not write new snapshot file to {}'
                         .format(self.snapshot_path))


class ArchiveOkConfig(RuntimeConfig):
    @classmethod
    def from_args(cls, args, allow_archive_configs=True):
        return super().from_args(
            args=args,
            allow_archive_configs=allow_archive_configs
        )


class MigrationTask(BaseTask):
    ConfigType = ArchiveOkConfig

    def __init__(self, args, config):
        if not args.from_archive:
            raise RuntimeException(
                'The --from-archive paramteter is required!'
            )
        if args.apply:
            args.write_files = True
            args.migrate_database = True
        super().__init__(args, config)

        self.snapshot_root = os.path.normpath(self.config.snapshot_paths[0])
        system.make_directory(self.snapshot_root)

        self.backups_made = []
        self.snapshots_written = []

    def archive_definitions(self, adapter):
        default_database = self.config.credentials.database
        for archive in self.config.archive:
            target_database = archive.get('target_database', default_database)
            target_schema = archive['target_schema']
            source_database = archive.get('source_database', default_database)
            source_schema = archive['source_schema']
            for table in archive['tables']:
                table_copy = table.copy()
                table_copy['target_database'] = target_database
                table_copy['target_schema'] = target_schema
                table_copy['source_database'] = source_database
                table_copy['source_schema'] = source_schema

                yield Migrator(self, adapter, table_copy)

    def perform_migration_with(self, adapter):
        archive_defs = list(self.archive_definitions(adapter))
        num_defs = len(archive_defs)
        logger.info('Found {} archive{} to migrate'.format(
            num_defs, '' if num_defs == 1 else 's'
        ))
        logger.info('')
        for idx, migrator in enumerate(archive_defs, start=1):
            logger.info('Archive {} of {}: {}'
                        .format(idx, len(archive_defs), migrator.relation))
            if self.args.migrate_database:
                migrator.migrate_archive_table()
                self.backups_made.append(migrator.backup)
            else:
                logger.info('  - Skipping migration in dry-run mode')
            if self.args.write_files:
                migrator.write_file()
                self.snapshots_written.append(migrator.snapshot_path)
            else:
                logger.info('  - Skipping new snapshot file in dry-run mode')
            logger.info('')

    def run(self):
        adapter = get_adapter(self.config)
        with adapter.connection_named('migration'):
            self.perform_migration_with(adapter)

        if self.backups_made:
            logger.info('The following backup tables were created:')
            for backup in self.backups_made:
                logger.info('  - {!s}'.format(backup))
            logger.info('')

        if self.snapshots_written:
            logger.info('The following snapshot files were created:')
            for written in self.snapshots_written:
                logger.info('  - {!s}'.format(written))
            logger.info('')

        applied_something = any((self.args.apply,
                                 self.args.migrate_database,
                                 self.args.write_files))
        if applied_something:
            msg = (
                'After verifying the migrated tables in the database, please '
                'drop the backup\ntables and remove any archive configs from '
                'your dbt_project.yml file.'
            )
        else:
            msg = 'Re-run this script with `--apply` to apply these migrations'
        logger.info(msg)
