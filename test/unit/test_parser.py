from mock import MagicMock
import unittest

import os

import dbt.flags
import dbt.parser


class ParserTest(unittest.TestCase):

    def find_input_by_name(self, models, name):
        return next(
            (model for model in models if model.get('name') == name),
            {})

    def setUp(self):
        dbt.flags.STRICT_MODE = True

        self.maxDiff = None

        self.root_project_config = {
            'name': 'root',
            'version': '0.1',
            'profile': 'test',
            'project-root': os.path.abspath('.'),
        }

        self.snowplow_project_config = {
            'name': 'snowplow',
            'version': '0.1',
            'project-root': os.path.abspath('./dbt_modules/snowplow'),
        }

        self.model_config = {
            'enabled': True,
            'materialized': 'view',
            'post-hook': [],
            'pre-hook': [],
            'vars': {},
        }

    def test__single_model(self):
        models = [{
            'name': 'model_one',
            'resource_type': 'model',
            'package_name': 'root',
            'root_path': '/usr/src/app',
            'path': 'model_one.sql',
            'raw_sql': ("select * from events"),
        }]

        self.assertEquals(
            dbt.parser.parse_sql_nodes(
                models,
                self.root_project_config,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {
                'model.root.model_one': {
                    'name': 'model_one',
                    'resource_type': 'model',
                    'unique_id': 'model.root.model_one',
                    'fqn': ['root', 'model_one'],
                    'empty': False,
                    'package_name': 'root',
                    'root_path': '/usr/src/app',
                    'depends_on': [],
                    'config': self.model_config,
                    'tags': [],
                    'path': 'model_one.sql',
                    'raw_sql': self.find_input_by_name(
                        models, 'model_one').get('raw_sql')
                }
            }
        )

    def test__single_model__nested_configuration(self):
        models = [{
            'name': 'model_one',
            'resource_type': 'model',
            'package_name': 'root',
            'root_path': '/usr/src/app',
            'path': 'nested/path/model_one.sql',
            'raw_sql': ("select * from events"),
        }]

        self.root_project_config = {
            'name': 'root',
            'version': '0.1',
            'profile': 'test',
            'project-root': os.path.abspath('.'),
            'models': {
                'materialized': 'ephemeral',
                'root': {
                    'nested': {
                        'path': {
                            'materialized': 'ephemeral'
                        }
                    }
                }
            }
        }

        ephemeral_config = self.model_config.copy()
        ephemeral_config.update({
            'materialized': 'ephemeral'
        })

        self.assertEquals(
            dbt.parser.parse_sql_nodes(
                models,
                self.root_project_config,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {
                'model.root.model_one': {
                    'name': 'model_one',
                    'resource_type': 'model',
                    'unique_id': 'model.root.model_one',
                    'fqn': ['root', 'nested', 'path', 'model_one'],
                    'empty': False,
                    'package_name': 'root',
                    'root_path': '/usr/src/app',
                    'depends_on': [],
                    'config': ephemeral_config,
                    'tags': [],
                    'path': 'nested/path/model_one.sql',
                    'raw_sql': self.find_input_by_name(
                        models, 'model_one').get('raw_sql')
                }
            }
        )

    def test__empty_model(self):
        models = [{
            'name': 'model_one',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'model_one.sql',
            'root_path': '/usr/src/app',
            'raw_sql': (" "),
        }]

        self.assertEquals(
            dbt.parser.parse_sql_nodes(
                models,
                self.root_project_config,
                {'root': self.root_project_config}),
            {
                'model.root.model_one': {
                    'name': 'model_one',
                    'resource_type': 'model',
                    'unique_id': 'model.root.model_one',
                    'fqn': ['root', 'model_one'],
                    'empty': True,
                    'package_name': 'root',
                    'depends_on': [],
                    'config': self.model_config,
                    'tags': [],
                    'path': 'model_one.sql',
                    'root_path': '/usr/src/app',
                    'raw_sql': self.find_input_by_name(
                        models, 'model_one').get('raw_sql')
                }
            }
        )

    def test__simple_dependency(self):
        models = [{
            'name': 'base',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'base.sql',
            'root_path': '/usr/src/app',
            'raw_sql': 'select * from events'
        }, {
            'name': 'events_tx',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'events_tx.sql',
            'root_path': '/usr/src/app',
            'raw_sql': "select * from {{ref('base')}}"
        }]

        self.assertEquals(
            dbt.parser.parse_sql_nodes(
                models,
                self.root_project_config,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {
                'model.root.base': {
                    'name': 'base',
                    'resource_type': 'model',
                    'unique_id': 'model.root.base',
                    'fqn': ['root', 'base'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': [],
                    'config': self.model_config,
                    'tags': [],
                    'path': 'base.sql',
                    'root_path': '/usr/src/app',
                    'raw_sql': self.find_input_by_name(
                        models, 'base').get('raw_sql')
                },
                'model.root.events_tx': {
                    'name': 'events_tx',
                    'resource_type': 'model',
                    'unique_id': 'model.root.events_tx',
                    'fqn': ['root', 'events_tx'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': [],
                    'config': self.model_config,
                    'tags': [],
                    'path': 'events_tx.sql',
                    'root_path': '/usr/src/app',
                    'raw_sql': self.find_input_by_name(
                        models, 'events_tx').get('raw_sql')
                }
            }
        )

    def test__multiple_dependencies(self):
        models = [{
            'name': 'events',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'events.sql',
            'root_path': '/usr/src/app',
            'raw_sql': 'select * from base.events',
        }, {
            'name': 'sessions',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'sessions.sql',
            'root_path': '/usr/src/app',
            'raw_sql': 'select * from base.sessions',
        }, {
            'name': 'events_tx',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'events_tx.sql',
            'root_path': '/usr/src/app',
            'raw_sql': ("with events as (select * from {{ref('events')}}) "
                        "select * from events"),
        }, {
            'name': 'sessions_tx',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'sessions_tx.sql',
            'root_path': '/usr/src/app',
            'raw_sql': ("with sessions as (select * from {{ref('sessions')}}) "
                        "select * from sessions"),
        }, {
            'name': 'multi',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'multi.sql',
            'root_path': '/usr/src/app',
            'raw_sql': ("with s as (select * from {{ref('sessions_tx')}}), "
                        "e as (select * from {{ref('events_tx')}}) "
                        "select * from e left join s on s.id = e.sid"),
        }]

        self.assertEquals(
            dbt.parser.parse_sql_nodes(
                models,
                self.root_project_config,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {
                'model.root.events': {
                    'name': 'events',
                    'resource_type': 'model',
                    'unique_id': 'model.root.events',
                    'fqn': ['root', 'events'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': [],
                    'config': self.model_config,
                    'tags': [],
                    'path': 'events.sql',
                    'root_path': '/usr/src/app',
                    'raw_sql': self.find_input_by_name(
                        models, 'events').get('raw_sql')
                },
                'model.root.sessions': {
                    'name': 'sessions',
                    'resource_type': 'model',
                    'unique_id': 'model.root.sessions',
                    'fqn': ['root', 'sessions'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': [],
                    'config': self.model_config,
                    'tags': [],
                    'path': 'sessions.sql',
                    'root_path': '/usr/src/app',
                    'raw_sql': self.find_input_by_name(
                        models, 'sessions').get('raw_sql')
                },
                'model.root.events_tx': {
                    'name': 'events_tx',
                    'resource_type': 'model',
                    'unique_id': 'model.root.events_tx',
                    'fqn': ['root', 'events_tx'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': [],
                    'config': self.model_config,
                    'tags': [],
                    'path': 'events_tx.sql',
                    'root_path': '/usr/src/app',
                    'raw_sql': self.find_input_by_name(
                        models, 'events_tx').get('raw_sql')
                },
                'model.root.sessions_tx': {
                    'name': 'sessions_tx',
                    'resource_type': 'model',
                    'unique_id': 'model.root.sessions_tx',
                    'fqn': ['root', 'sessions_tx'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': [],
                    'config': self.model_config,
                    'tags': [],
                    'path': 'sessions_tx.sql',
                    'root_path': '/usr/src/app',
                    'raw_sql': self.find_input_by_name(
                        models, 'sessions_tx').get('raw_sql')
                },
                'model.root.multi': {
                    'name': 'multi',
                    'resource_type': 'model',
                    'unique_id': 'model.root.multi',
                    'fqn': ['root', 'multi'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': [],
                    'config': self.model_config,
                    'tags': [],
                    'path': 'multi.sql',
                    'root_path': '/usr/src/app',
                    'raw_sql': self.find_input_by_name(
                        models, 'multi').get('raw_sql')
                }
            }
        )

    def test__multiple_dependencies__packages(self):
        models = [{
            'name': 'events',
            'resource_type': 'model',
            'package_name': 'snowplow',
            'path': 'events.sql',
            'root_path': '/usr/src/app',
            'raw_sql': 'select * from base.events',
        }, {
            'name': 'sessions',
            'resource_type': 'model',
            'package_name': 'snowplow',
            'path': 'sessions.sql',
            'root_path': '/usr/src/app',
            'raw_sql': 'select * from base.sessions',
        }, {
            'name': 'events_tx',
            'resource_type': 'model',
            'package_name': 'snowplow',
            'path': 'events_tx.sql',
            'root_path': '/usr/src/app',
            'raw_sql': ("with events as (select * from {{ref('events')}}) "
                        "select * from events"),
        }, {
            'name': 'sessions_tx',
            'resource_type': 'model',
            'package_name': 'snowplow',
            'path': 'sessions_tx.sql',
            'root_path': '/usr/src/app',
            'raw_sql': ("with sessions as (select * from {{ref('sessions')}}) "
                        "select * from sessions"),
        }, {
            'name': 'multi',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'multi.sql',
            'root_path': '/usr/src/app',
            'raw_sql': ("with s as (select * from {{ref('snowplow', 'sessions_tx')}}), "
                        "e as (select * from {{ref('snowplow', 'events_tx')}}) "
                        "select * from e left join s on s.id = e.sid"),
        }]

        self.assertEquals(
            dbt.parser.parse_sql_nodes(
                models,
                self.root_project_config,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {
                'model.snowplow.events': {
                    'name': 'events',
                    'resource_type': 'model',
                    'unique_id': 'model.snowplow.events',
                    'fqn': ['snowplow', 'events'],
                    'empty': False,
                    'package_name': 'snowplow',
                    'depends_on': [],
                    'config': self.model_config,
                    'tags': [],
                    'path': 'events.sql',
                    'root_path': '/usr/src/app',
                    'raw_sql': self.find_input_by_name(
                        models, 'events').get('raw_sql')
                },
                'model.snowplow.sessions': {
                    'name': 'sessions',
                    'resource_type': 'model',
                    'unique_id': 'model.snowplow.sessions',
                    'fqn': ['snowplow', 'sessions'],
                    'empty': False,
                    'package_name': 'snowplow',
                    'depends_on': [],
                    'config': self.model_config,
                    'tags': [],
                    'path': 'sessions.sql',
                    'root_path': '/usr/src/app',
                    'raw_sql': self.find_input_by_name(
                        models, 'sessions').get('raw_sql')
                },
                'model.snowplow.events_tx': {
                    'name': 'events_tx',
                    'resource_type': 'model',
                    'unique_id': 'model.snowplow.events_tx',
                    'fqn': ['snowplow', 'events_tx'],
                    'empty': False,
                    'package_name': 'snowplow',
                    'depends_on': [],
                    'config': self.model_config,
                    'tags': [],
                    'path': 'events_tx.sql',
                    'root_path': '/usr/src/app',
                    'raw_sql': self.find_input_by_name(
                        models, 'events_tx').get('raw_sql')
                },
                'model.snowplow.sessions_tx': {
                    'name': 'sessions_tx',
                    'resource_type': 'model',
                    'unique_id': 'model.snowplow.sessions_tx',
                    'fqn': ['snowplow', 'sessions_tx'],
                    'empty': False,
                    'package_name': 'snowplow',
                    'depends_on': [],
                    'config': self.model_config,
                    'tags': [],
                    'path': 'sessions_tx.sql',
                    'root_path': '/usr/src/app',
                    'raw_sql': self.find_input_by_name(
                        models, 'sessions_tx').get('raw_sql')
                },
                'model.root.multi': {
                    'name': 'multi',
                    'resource_type': 'model',
                    'unique_id': 'model.root.multi',
                    'fqn': ['root', 'multi'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': [],
                    'config': self.model_config,
                    'tags': [],
                    'path': 'multi.sql',
                    'root_path': '/usr/src/app',
                    'raw_sql': self.find_input_by_name(
                        models, 'multi').get('raw_sql')
                }
            }
        )

    def test__in_model_config(self):
        models = [{
            'name': 'model_one',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'model_one.sql',
            'root_path': '/usr/src/app',
            'raw_sql': ("{{config({'materialized':'table'})}}"
                        "select * from events"),
        }]

        self.model_config.update({
            'materialized': 'table'
        })

        self.assertEquals(
            dbt.parser.parse_sql_nodes(
                models,
                self.root_project_config,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {
                'model.root.model_one': {
                    'name': 'model_one',
                    'resource_type': 'model',
                    'unique_id': 'model.root.model_one',
                    'fqn': ['root', 'model_one'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': [],
                    'config': self.model_config,
                    'tags': [],
                    'root_path': '/usr/src/app',
                    'path': 'model_one.sql',
                    'raw_sql': self.find_input_by_name(
                        models, 'model_one').get('raw_sql')
                }
            }
        )

    def test__root_project_config(self):
        self.root_project_config = {
            'name': 'root',
            'version': '0.1',
            'profile': 'test',
            'project-root': os.path.abspath('.'),
            'models': {
                'materialized': 'ephemeral',
                'root': {
                    'view': {
                        'materialized': 'view'
                    }
                }
            }
        }

        models = [{
            'name': 'table',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'table.sql',
            'root_path': '/usr/src/app',
            'raw_sql': ("{{config({'materialized':'table'})}}"
                        "select * from events"),
        }, {
            'name': 'ephemeral',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'ephemeral.sql',
            'root_path': '/usr/src/app',
            'raw_sql': ("select * from events"),
        }, {
            'name': 'view',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'view.sql',
            'root_path': '/usr/src/app',
            'raw_sql': ("select * from events"),
        }]

        self.model_config.update({
            'materialized': 'table'
        })

        ephemeral_config = self.model_config.copy()
        ephemeral_config.update({
            'materialized': 'ephemeral'
        })

        view_config = self.model_config.copy()
        view_config.update({
            'materialized': 'view'
        })

        self.assertEquals(
            dbt.parser.parse_sql_nodes(
                models,
                self.root_project_config,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {
                'model.root.table': {
                    'name': 'table',
                    'resource_type': 'model',
                    'unique_id': 'model.root.table',
                    'fqn': ['root', 'table'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': [],
                    'path': 'table.sql',
                    'config': self.model_config,
                    'tags': [],
                    'root_path': '/usr/src/app',
                    'raw_sql': self.find_input_by_name(
                        models, 'table').get('raw_sql')
                },
                'model.root.ephemeral': {
                    'name': 'ephemeral',
                    'resource_type': 'model',
                    'unique_id': 'model.root.ephemeral',
                    'fqn': ['root', 'ephemeral'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': [],
                    'path': 'ephemeral.sql',
                    'config': ephemeral_config,
                    'tags': [],
                    'root_path': '/usr/src/app',
                    'raw_sql': self.find_input_by_name(
                        models, 'ephemeral').get('raw_sql')
                },
                'model.root.view': {
                    'name': 'view',
                    'resource_type': 'model',
                    'unique_id': 'model.root.view',
                    'fqn': ['root', 'view'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': [],
                    'path': 'view.sql',
                    'root_path': '/usr/src/app',
                    'config': view_config,
                    'tags': [],
                    'raw_sql': self.find_input_by_name(
                        models, 'ephemeral').get('raw_sql')
                }
            }

        )

    def test__other_project_config(self):
        self.root_project_config = {
            'name': 'root',
            'version': '0.1',
            'profile': 'test',
            'project-root': os.path.abspath('.'),
            'models': {
                'materialized': 'ephemeral',
                'root': {
                    'view': {
                        'materialized': 'view'
                    }
                },
                'snowplow': {
                    'enabled': False,
                    'views': {
                        'materialized': 'view',
                    }
                }
            }
        }

        self.snowplow_project_config = {
            'name': 'snowplow',
            'version': '0.1',
            'project-root': os.path.abspath('./dbt_modules/snowplow'),
            'models': {
                'snowplow': {
                    'enabled': False,
                    'views': {
                        'materialized': 'table',
                        'sort': 'timestamp'
                    }
                }
            }
        }

        models = [{
            'name': 'table',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'table.sql',
            'root_path': '/usr/src/app',
            'raw_sql': ("{{config({'materialized':'table'})}}"
                        "select * from events"),
        }, {
            'name': 'ephemeral',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'ephemeral.sql',
            'root_path': '/usr/src/app',
            'raw_sql': ("select * from events"),
        }, {
            'name': 'view',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'view.sql',
            'root_path': '/usr/src/app',
            'raw_sql': ("select * from events"),
        }, {
            'name': 'disabled',
            'resource_type': 'model',
            'package_name': 'snowplow',
            'path': 'disabled.sql',
            'root_path': '/usr/src/app',
            'raw_sql': ("select * from events"),
        }, {
            'name': 'package',
            'resource_type': 'model',
            'package_name': 'snowplow',
            'path': 'views/package.sql',
            'root_path': '/usr/src/app',
            'raw_sql': ("select * from events"),
        }]

        self.model_config.update({
            'materialized': 'table'
        })

        ephemeral_config = self.model_config.copy()
        ephemeral_config.update({
            'materialized': 'ephemeral'
        })

        view_config = self.model_config.copy()
        view_config.update({
            'materialized': 'view'
        })

        disabled_config = self.model_config.copy()
        disabled_config.update({
            'enabled': False,
            'materialized': 'ephemeral'
        })

        sort_config = self.model_config.copy()
        sort_config.update({
            'enabled': False,
            'materialized': 'view',
            'sort': 'timestamp',
        })

        self.assertEquals(
            dbt.parser.parse_sql_nodes(
                models,
                self.root_project_config,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {
                'model.root.table': {
                    'name': 'table',
                    'resource_type': 'model',
                    'unique_id': 'model.root.table',
                    'fqn': ['root', 'table'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': [],
                    'path': 'table.sql',
                    'root_path': '/usr/src/app',
                    'config': self.model_config,
                    'tags': [],
                    'raw_sql': self.find_input_by_name(
                        models, 'table').get('raw_sql')
                },
                'model.root.ephemeral': {
                    'name': 'ephemeral',
                    'resource_type': 'model',
                    'unique_id': 'model.root.ephemeral',
                    'fqn': ['root', 'ephemeral'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': [],
                    'path': 'ephemeral.sql',
                    'root_path': '/usr/src/app',
                    'config': ephemeral_config,
                    'tags': [],
                    'raw_sql': self.find_input_by_name(
                        models, 'ephemeral').get('raw_sql')
                },
                'model.root.view': {
                    'name': 'view',
                    'resource_type': 'model',
                    'unique_id': 'model.root.view',
                    'fqn': ['root', 'view'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': [],
                    'path': 'view.sql',
                    'root_path': '/usr/src/app',
                    'config': view_config,
                    'tags': [],
                    'raw_sql': self.find_input_by_name(
                        models, 'view').get('raw_sql')
                },
                'model.snowplow.disabled': {
                    'name': 'disabled',
                    'resource_type': 'model',
                    'unique_id': 'model.snowplow.disabled',
                    'fqn': ['snowplow', 'disabled'],
                    'empty': False,
                    'package_name': 'snowplow',
                    'depends_on': [],
                    'path': 'disabled.sql',
                    'root_path': '/usr/src/app',
                    'config': disabled_config,
                    'tags': [],
                    'raw_sql': self.find_input_by_name(
                        models, 'disabled').get('raw_sql')
                },
                'model.snowplow.package': {
                    'name': 'package',
                    'resource_type': 'model',
                    'unique_id': 'model.snowplow.package',
                    'fqn': ['snowplow', 'views', 'package'],
                    'empty': False,
                    'package_name': 'snowplow',
                    'depends_on': [],
                    'path': 'views/package.sql',
                    'root_path': '/usr/src/app',
                    'config': sort_config,
                    'tags': [],
                    'raw_sql': self.find_input_by_name(
                        models, 'package').get('raw_sql')
                }
            }
        )

    def test__simple_schema_test(self):
        tests = [{
            'name': 'test_one',
            'resource_type': 'test',
            'package_name': 'root',
            'root_path': '/usr/src/app',
            'path': 'test_one.yml',
            'raw_sql': None,
            'raw_yml': ('{model_one: {constraints: {not_null: [id],'
                        'unique: [id],'
                        'accepted_values: [{field: id, values: ["a","b"]}],'
                        'relationships: [{from: id, to: model_two, field: id}]'
                        '}}}')
        }]

        not_null_sql = dbt.parser.QUERY_VALIDATE_NOT_NULL \
                                 .format(
                                     field='id',
                                     ref="{{ref('model_one')}}")

        unique_sql = dbt.parser.QUERY_VALIDATE_UNIQUE \
                               .format(
                                   field='id',
                                   ref="{{ref('model_one')}}")

        accepted_values_sql = dbt.parser.QUERY_VALIDATE_ACCEPTED_VALUES \
                                        .format(
                                            field='id',
                                            ref="{{ref('model_one')}}",
                                            values_csv="'a','b'")

        relationships_sql = dbt.parser.QUERY_VALIDATE_REFERENTIAL_INTEGRITY \
                                        .format(
                                            parent_field='id',
                                            parent_ref="{{ref('model_two')}}",
                                            child_field='id',
                                            child_ref="{{ref('model_one')}}")

        self.assertEquals(
            dbt.parser.parse_schema_tests(
                tests,
                self.root_project_config,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {
                'test.root.not_null_model_one_id': {
                    'name': 'not_null_model_one_id',
                    'resource_type': 'test',
                    'unique_id': 'test.root.not_null_model_one_id',
                    'fqn': ['root', 'schema', 'test_one'],
                    'empty': False,
                    'package_name': 'root',
                    'root_path': '/usr/src/app',
                    'depends_on': [],
                    'config': self.model_config,
                    'path': 'test_one.yml',
                    'tags': ['schema'],
                    'raw_sql': not_null_sql,
                },
                'test.root.unique_model_one_id': {
                    'name': 'unique_model_one_id',
                    'resource_type': 'test',
                    'unique_id': 'test.root.unique_model_one_id',
                    'fqn': ['root', 'schema', 'test_one'],
                    'empty': False,
                    'package_name': 'root',
                    'root_path': '/usr/src/app',
                    'depends_on': [],
                    'config': self.model_config,
                    'path': 'test_one.yml',
                    'tags': ['schema'],
                    'raw_sql': unique_sql,
                },
                'test.root.accepted_values_model_one_id': {
                    'name': 'accepted_values_model_one_id',
                    'resource_type': 'test',
                    'unique_id': 'test.root.accepted_values_model_one_id',
                    'fqn': ['root', 'schema', 'test_one'],
                    'empty': False,
                    'package_name': 'root',
                    'root_path': '/usr/src/app',
                    'depends_on': [],
                    'config': self.model_config,
                    'path': 'test_one.yml',
                    'tags': ['schema'],
                    'raw_sql': accepted_values_sql,
                },
                'test.root.relationships_model_one_id_to_model_two_id': {
                    'name': 'relationships_model_one_id_to_model_two_id',
                    'resource_type': 'test',
                    'unique_id': 'test.root.relationships_model_one_id_to_model_two_id',
                    'fqn': ['root', 'schema', 'test_one'],
                    'empty': False,
                    'package_name': 'root',
                    'root_path': '/usr/src/app',
                    'depends_on': [],
                    'config': self.model_config,
                    'path': 'test_one.yml',
                    'tags': ['schema'],
                    'raw_sql': relationships_sql,
                }


            }
        )


    def test__simple_data_test(self):
        tests = [{
            'name': 'no_events',
            'resource_type': 'test',
            'package_name': 'root',
            'path': 'no_events.sql',
            'root_path': '/usr/src/app',
            'raw_sql': "select * from {{ref('base')}}"
        }]

        self.assertEquals(
            dbt.parser.parse_sql_nodes(
                tests,
                self.root_project_config,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {
                'test.root.no_events': {
                    'name': 'no_events',
                    'resource_type': 'test',
                    'unique_id': 'test.root.no_events',
                    'fqn': ['root', 'no_events'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': [],
                    'config': self.model_config,
                    'path': 'no_events.sql',
                    'root_path': '/usr/src/app',
                    'tags': [],
                    'raw_sql': self.find_input_by_name(
                        tests, 'no_events').get('raw_sql')
                }
            }
        )
