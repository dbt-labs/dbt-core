from mock import MagicMock
import unittest

import os

import dbt.parser


class ParserTest(unittest.TestCase):

    def find_input_by_name(self, models, name):
        return next(
            (model for model in models if model.get('name') == name),
            {})

    def setUp(self):
        self.maxDiff = None

        self.root_project_config = {
            'name': 'root_project',
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
            'package_name': 'root',
            'raw_sql': ("select * from events"),
        }]

        self.assertEquals(
            dbt.parser.parse_models(
                models,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {
                'models.root.model_one': {
                    'name': 'model_one',
                    'fqn': ['root_project', 'model_one'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': [],
                    'config': self.model_config,
                    'raw_sql': self.find_input_by_name(
                        models, 'model_one').get('raw_sql')
                }
            }
        )

    def test__empty_model(self):
        models = [{
            'name': 'model_one',
            'package_name': 'root',
            'raw_sql': (" "),
        }]

        self.assertEquals(
            dbt.parser.parse_models(
                models,
                {'root': self.root_project_config}),
            {
                'models.root.model_one': {
                    'name': 'model_one',
                    'fqn': ['root_project', 'model_one'],
                    'empty': True,
                    'package_name': 'root',
                    'depends_on': [],
                    'config': self.model_config,
                    'raw_sql': self.find_input_by_name(
                        models, 'model_one').get('raw_sql')
                }
            }
        )

    def test__simple_dependency(self):
        models = [{
            'name': 'base',
            'package_name': 'root',
            'raw_sql': 'select * from events'
        }, {
            'name': 'events_tx',
            'package_name': 'root',
            'raw_sql': "select * from {{ref('base')}}"
        }]

        self.assertEquals(
            dbt.parser.parse_models(
                models,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {
                'models.root.base': {
                    'name': 'base',
                    'fqn': ['root_project', 'base'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': [],
                    'config': self.model_config,
                    'raw_sql': self.find_input_by_name(
                        models, 'base').get('raw_sql')
                },
                'models.root.events_tx': {
                    'name': 'events_tx',
                    'fqn': ['root_project', 'events_tx'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': ['models.root.base'],
                    'config': self.model_config,
                    'raw_sql': self.find_input_by_name(
                        models, 'events_tx').get('raw_sql')
                }
            }
        )

    def test__multiple_dependencies(self):
        models = [{
            'name': 'events',
            'package_name': 'root',
            'raw_sql': 'select * from base.events',
        }, {
            'name': 'sessions',
            'package_name': 'root',
            'raw_sql': 'select * from base.sessions',
        }, {
            'name': 'events_tx',
            'package_name': 'root',
            'raw_sql': ("with events as (select * from {{ref('events')}}) "
                        "select * from events"),
        }, {
            'name': 'sessions_tx',
            'package_name': 'root',
            'raw_sql': ("with sessions as (select * from {{ref('sessions')}}) "
                        "select * from sessions"),
        }, {
            'name': 'multi',
            'package_name': 'root',
            'raw_sql': ("with s as (select * from {{ref('sessions_tx')}}), "
                        "e as (select * from {{ref('events_tx')}}) "
                        "select * from e left join s on s.id = e.sid"),
        }]

        self.assertEquals(
            dbt.parser.parse_models(
                models,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {
                'models.root.events': {
                    'name': 'events',
                    'fqn': ['root_project', 'events'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': [],
                    'config': self.model_config,
                    'raw_sql': self.find_input_by_name(
                        models, 'events').get('raw_sql')
                },
                'models.root.sessions': {
                    'name': 'sessions',
                    'fqn': ['root_project', 'sessions'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': [],
                    'config': self.model_config,
                    'raw_sql': self.find_input_by_name(
                        models, 'sessions').get('raw_sql')
                },
                'models.root.events_tx': {
                    'name': 'events_tx',
                    'fqn': ['root_project', 'events_tx'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': ['models.root.events'],
                    'config': self.model_config,
                    'raw_sql': self.find_input_by_name(
                        models, 'events_tx').get('raw_sql')
                },
                'models.root.sessions_tx': {
                    'name': 'sessions_tx',
                    'fqn': ['root_project', 'sessions_tx'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': ['models.root.sessions'],
                    'config': self.model_config,
                    'raw_sql': self.find_input_by_name(
                        models, 'sessions_tx').get('raw_sql')
                },
                'models.root.multi': {
                    'name': 'multi',
                    'fqn': ['root_project', 'multi'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': ['models.root.sessions_tx',
                                   'models.root.events_tx'],
                    'config': self.model_config,
                    'raw_sql': self.find_input_by_name(
                        models, 'multi').get('raw_sql')
                }
            }
        )

    def test__multiple_dependencies__packages(self):
        models = [{
            'name': 'events',
            'package_name': 'snowplow',
            'raw_sql': 'select * from base.events',
        }, {
            'name': 'sessions',
            'package_name': 'snowplow',
            'raw_sql': 'select * from base.sessions',
        }, {
            'name': 'events_tx',
            'package_name': 'snowplow',
            'raw_sql': ("with events as (select * from {{ref('events')}}) "
                        "select * from events"),
        }, {
            'name': 'sessions_tx',
            'package_name': 'snowplow',
            'raw_sql': ("with sessions as (select * from {{ref('sessions')}}) "
                        "select * from sessions"),
        }, {
            'name': 'multi',
            'package_name': 'root',
            'raw_sql': ("with s as (select * from {{ref('snowplow', 'sessions_tx')}}), "
                        "e as (select * from {{ref('snowplow', 'events_tx')}}) "
                        "select * from e left join s on s.id = e.sid"),
        }]

        self.assertEquals(
            dbt.parser.parse_models(
                models,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {
                'models.snowplow.events': {
                    'name': 'events',
                    'fqn': ['snowplow', 'events'],
                    'empty': False,
                    'package_name': 'snowplow',
                    'depends_on': [],
                    'config': self.model_config,
                    'raw_sql': self.find_input_by_name(
                        models, 'events').get('raw_sql')
                },
                'models.snowplow.sessions': {
                    'name': 'sessions',
                    'fqn': ['snowplow', 'sessions'],
                    'empty': False,
                    'package_name': 'snowplow',
                    'depends_on': [],
                    'config': self.model_config,
                    'raw_sql': self.find_input_by_name(
                        models, 'sessions').get('raw_sql')
                },
                'models.snowplow.events_tx': {
                    'name': 'events_tx',
                    'fqn': ['snowplow', 'events_tx'],
                    'empty': False,
                    'package_name': 'snowplow',
                    'depends_on': ['models.snowplow.events'],
                    'config': self.model_config,
                    'raw_sql': self.find_input_by_name(
                        models, 'events_tx').get('raw_sql')
                },
                'models.snowplow.sessions_tx': {
                    'name': 'sessions_tx',
                    'fqn': ['snowplow', 'sessions_tx'],
                    'empty': False,
                    'package_name': 'snowplow',
                    'depends_on': ['models.snowplow.sessions'],
                    'config': self.model_config,
                    'raw_sql': self.find_input_by_name(
                        models, 'sessions_tx').get('raw_sql')
                },
                'models.root.multi': {
                    'name': 'multi',
                    'fqn': ['root_project', 'multi'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': ['models.snowplow.sessions_tx',
                                   'models.snowplow.events_tx'],
                    'config': self.model_config,
                    'raw_sql': self.find_input_by_name(
                        models, 'multi').get('raw_sql')
                }
            }
        )

    def test__in_model_config(self):
        models = [{
            'name': 'model_one',
            'package_name': 'root',
            'raw_sql': ("{{config({'materialized':'table'})}}"
                        "select * from events"),
        }]

        self.model_config.update({
            'materialized': 'table'
        })

        self.assertEquals(
            dbt.parser.parse_models(
                models,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {
                'models.root.model_one': {
                    'name': 'model_one',
                    'fqn': ['root_project', 'model_one'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': [],
                    'config': self.model_config,
                    'raw_sql': self.find_input_by_name(
                        models, 'model_one').get('raw_sql')
                }
            }
        )

    def test__root_project_config(self):
        self.root_project_config = {
            'name': 'root_project',
            'version': '0.1',
            'profile': 'test',
            'project-root': os.path.abspath('.'),
            'models': {
                'materialized': 'ephemeral',
                'root_project': {
                    'view': {
                        'materialized': 'view'
                    }
                }
            }
        }

        models = [{
            'name': 'table',
            'package_name': 'root',
            'path': 'table.sql',
            'raw_sql': ("{{config({'materialized':'table'})}}"
                        "select * from events"),
        }, {
            'name': 'ephemeral',
            'package_name': 'root',
            'path': 'ephemeral.sql',
            'raw_sql': ("select * from events"),
        }, {
            'name': 'view',
            'package_name': 'root',
            'path': 'view.sql',
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
            dbt.parser.parse_models(
                models,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {
                'models.root.table': {
                    'name': 'table',
                    'fqn': ['root_project', 'table'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': [],
                    'path': 'table.sql',
                    'config': self.model_config,
                    'raw_sql': self.find_input_by_name(
                        models, 'table').get('raw_sql')
                },
                'models.root.ephemeral': {
                    'name': 'ephemeral',
                    'fqn': ['root_project', 'ephemeral'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': [],
                    'path': 'ephemeral.sql',
                    'config': ephemeral_config,
                    'raw_sql': self.find_input_by_name(
                        models, 'ephemeral').get('raw_sql')
                },
                'models.root.view': {
                    'name': 'view',
                    'fqn': ['root_project', 'view'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': [],
                    'path': 'view.sql',
                    'config': view_config,
                    'raw_sql': self.find_input_by_name(
                        models, 'ephemeral').get('raw_sql')
                }
            }

        )

    def test__other_project_config(self):
        self.root_project_config = {
            'name': 'root_project',
            'version': '0.1',
            'profile': 'test',
            'project-root': os.path.abspath('.'),
            'models': {
                'materialized': 'ephemeral',
                'root_project': {
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
                'enabled': False,
                'views': {
                    'materialized': 'table',
                    'sort': 'timestamp'
                }
            }
        }

        models = [{
            'name': 'table',
            'package_name': 'root',
            'path': 'table.sql',
            'raw_sql': ("{{config({'materialized':'table'})}}"
                        "select * from events"),
        }, {
            'name': 'ephemeral',
            'package_name': 'root',
            'path': 'ephemeral.sql',
            'raw_sql': ("select * from events"),
        }, {
            'name': 'view',
            'package_name': 'root',
            'path': 'view.sql',
            'raw_sql': ("select * from events"),
        }, {
            'name': 'disabled',
            'package_name': 'snowplow',
            'path': 'disabled.sql',
            'raw_sql': ("select * from events"),
        }, {
            'name': 'package',
            'package_name': 'snowplow',
            'path': 'models/views/package.sql',
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
            'materialized': 'view'
        })

        self.assertEquals(
            dbt.parser.parse_models(
                models,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {
                'models.root.table': {
                    'name': 'table',
                    'fqn': ['root_project', 'table'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': [],
                    'path': 'table.sql',
                    'config': self.model_config,
                    'raw_sql': self.find_input_by_name(
                        models, 'table').get('raw_sql')
                },
                'models.root.ephemeral': {
                    'name': 'ephemeral',
                    'fqn': ['root_project', 'ephemeral'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': [],
                    'path': 'ephemeral.sql',
                    'config': ephemeral_config,
                    'raw_sql': self.find_input_by_name(
                        models, 'ephemeral').get('raw_sql')
                },
                'models.root.view': {
                    'name': 'view',
                    'fqn': ['root_project', 'view'],
                    'empty': False,
                    'package_name': 'root',
                    'depends_on': [],
                    'path': 'view.sql',
                    'config': view_config,
                    'raw_sql': self.find_input_by_name(
                        models, 'view').get('raw_sql')
                },
                'models.snowplow.disabled': {
                    'name': 'disabled',
                    'fqn': ['snowplow', 'disabled'],
                    'empty': False,
                    'package_name': 'snowplow',
                    'depends_on': [],
                    'path': 'disabled.sql',
                    'config': disabled_config,
                    'raw_sql': self.find_input_by_name(
                        models, 'disabled').get('raw_sql')
                },
                'models.snowplow.package': {
                    'name': 'package',
                    'fqn': ['snowplow', 'views', 'package'],
                    'empty': False,
                    'package_name': 'snowplow',
                    'depends_on': [],
                    'path': 'models/views/package.sql',
                    'config': sort_config,
                    'raw_sql': self.find_input_by_name(
                        models, 'package').get('raw_sql')
                }
            }
        )
