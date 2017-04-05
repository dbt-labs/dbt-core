import unittest

import jinja2.runtime
import os

import dbt.flags
import dbt.parser


def get_os_path(unix_path):
    return os.path.normpath(unix_path)


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
            'root_path': get_os_path('/usr/src/app'),
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
                    'root_path': get_os_path('/usr/src/app'),
                    'refs': [],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'config': self.model_config,
                    'tags': set(),
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
            'root_path': get_os_path('/usr/src/app'),
            'path': get_os_path('nested/path/model_one.sql'),
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
                    'root_path': get_os_path('/usr/src/app'),
                    'refs': [],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'config': ephemeral_config,
                    'tags': set(),
                    'path': get_os_path('nested/path/model_one.sql'),
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
            'root_path': get_os_path('/usr/src/app'),
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
                    'refs': [],
                    'depends_on': {
                        'nodes': [],
                        'macros': [],
                    },
                    'config': self.model_config,
                    'tags': set(),
                    'path': 'model_one.sql',
                    'root_path': get_os_path('/usr/src/app'),
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
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': 'select * from events'
        }, {
            'name': 'events_tx',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'events_tx.sql',
            'root_path': get_os_path('/usr/src/app'),
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
                    'refs': [],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'config': self.model_config,
                    'tags': set(),
                    'path': 'base.sql',
                    'root_path': get_os_path('/usr/src/app'),
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
                    'refs': [('base',)],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'config': self.model_config,
                    'tags': set(),
                    'path': 'events_tx.sql',
                    'root_path': get_os_path('/usr/src/app'),
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
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': 'select * from base.events',
        }, {
            'name': 'sessions',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'sessions.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': 'select * from base.sessions',
        }, {
            'name': 'events_tx',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'events_tx.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("with events as (select * from {{ref('events')}}) "
                        "select * from events"),
        }, {
            'name': 'sessions_tx',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'sessions_tx.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("with sessions as (select * from {{ref('sessions')}}) "
                        "select * from sessions"),
        }, {
            'name': 'multi',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'multi.sql',
            'root_path': get_os_path('/usr/src/app'),
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
                    'refs': [],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'config': self.model_config,
                    'tags': set(),
                    'path': 'events.sql',
                    'root_path': get_os_path('/usr/src/app'),
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
                    'refs': [],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'config': self.model_config,
                    'tags': set(),
                    'path': 'sessions.sql',
                    'root_path': get_os_path('/usr/src/app'),
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
                    'refs': [('events',)],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'config': self.model_config,
                    'tags': set(),
                    'path': 'events_tx.sql',
                    'root_path': get_os_path('/usr/src/app'),
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
                    'refs': [('sessions',)],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'config': self.model_config,
                    'tags': set(),
                    'path': 'sessions_tx.sql',
                    'root_path': get_os_path('/usr/src/app'),
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
                    'refs': [('sessions_tx',), ('events_tx',)],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'config': self.model_config,
                    'tags': set(),
                    'path': 'multi.sql',
                    'root_path': get_os_path('/usr/src/app'),
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
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': 'select * from base.events',
        }, {
            'name': 'sessions',
            'resource_type': 'model',
            'package_name': 'snowplow',
            'path': 'sessions.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': 'select * from base.sessions',
        }, {
            'name': 'events_tx',
            'resource_type': 'model',
            'package_name': 'snowplow',
            'path': 'events_tx.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("with events as (select * from {{ref('events')}}) "
                        "select * from events"),
        }, {
            'name': 'sessions_tx',
            'resource_type': 'model',
            'package_name': 'snowplow',
            'path': 'sessions_tx.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("with sessions as (select * from {{ref('sessions')}}) "
                        "select * from sessions"),
        }, {
            'name': 'multi',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'multi.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("with s as "
                        "(select * from {{ref('snowplow', 'sessions_tx')}}), "
                        "e as "
                        "(select * from {{ref('snowplow', 'events_tx')}}) "
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
                    'refs': [],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'config': self.model_config,
                    'tags': set(),
                    'path': 'events.sql',
                    'root_path': get_os_path('/usr/src/app'),
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
                    'refs': [],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'config': self.model_config,
                    'tags': set(),
                    'path': 'sessions.sql',
                    'root_path': get_os_path('/usr/src/app'),
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
                    'refs': [('events',)],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'config': self.model_config,
                    'tags': set(),
                    'path': 'events_tx.sql',
                    'root_path': get_os_path('/usr/src/app'),
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
                    'refs': [('sessions',)],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'config': self.model_config,
                    'tags': set(),
                    'path': 'sessions_tx.sql',
                    'root_path': get_os_path('/usr/src/app'),
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
                    'refs': [('snowplow', 'sessions_tx'),
                             ('snowplow', 'events_tx')],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'config': self.model_config,
                    'tags': set(),
                    'path': 'multi.sql',
                    'root_path': get_os_path('/usr/src/app'),
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
            'root_path': get_os_path('/usr/src/app'),
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
                    'refs': [],
                    'depends_on': {
                        'nodes': [],
                        'macros': [],
                    },
                    'config': self.model_config,
                    'tags': set(),
                    'root_path': get_os_path('/usr/src/app'),
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
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("{{config({'materialized':'table'})}}"
                        "select * from events"),
        }, {
            'name': 'ephemeral',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'ephemeral.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("select * from events"),
        }, {
            'name': 'view',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'view.sql',
            'root_path': get_os_path('/usr/src/app'),
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
                    'refs': [],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'path': 'table.sql',
                    'config': self.model_config,
                    'tags': set(),
                    'root_path': get_os_path('/usr/src/app'),
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
                    'refs': [],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'path': 'ephemeral.sql',
                    'config': ephemeral_config,
                    'tags': set(),
                    'root_path': get_os_path('/usr/src/app'),
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
                    'refs': [],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'path': 'view.sql',
                    'root_path': get_os_path('/usr/src/app'),
                    'config': view_config,
                    'tags': set(),
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
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("{{config({'materialized':'table'})}}"
                        "select * from events"),
        }, {
            'name': 'ephemeral',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'ephemeral.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("select * from events"),
        }, {
            'name': 'view',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'view.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("select * from events"),
        }, {
            'name': 'disabled',
            'resource_type': 'model',
            'package_name': 'snowplow',
            'path': 'disabled.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("select * from events"),
        }, {
            'name': 'package',
            'resource_type': 'model',
            'package_name': 'snowplow',
            'path': get_os_path('views/package.sql'),
            'root_path': get_os_path('/usr/src/app'),
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
                    'refs': [],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'path': 'table.sql',
                    'root_path': get_os_path('/usr/src/app'),
                    'config': self.model_config,
                    'tags': set(),
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
                    'refs': [],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'path': 'ephemeral.sql',
                    'root_path': get_os_path('/usr/src/app'),
                    'config': ephemeral_config,
                    'tags': set(),
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
                    'refs': [],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'path': 'view.sql',
                    'root_path': get_os_path('/usr/src/app'),
                    'config': view_config,
                    'tags': set(),
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
                    'refs': [],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'path': 'disabled.sql',
                    'root_path': get_os_path('/usr/src/app'),
                    'config': disabled_config,
                    'tags': set(),
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
                    'refs': [],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'path': get_os_path('views/package.sql'),
                    'root_path': get_os_path('/usr/src/app'),
                    'config': sort_config,
                    'tags': set(),
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
            'root_path': get_os_path('/usr/src/app'),
            'path': 'test_one.yml',
            'raw_sql': None,
            'raw_yml': ('{model_one: {constraints: {not_null: [id],'
                        'unique: [id],'
                        'accepted_values: [{field: id, values: ["a","b"]}],'
                        'relationships: [{from: id, to: ref(\'model_two\'), field: id}]' # noqa
                        '}}}')
        }]

        not_null_sql = "{{ test_not_null(model=ref('model_one'), arg='id') }}"
        unique_sql = "{{ test_unique(model=ref('model_one'), arg='id') }}"
        accepted_values_sql = "{{ test_accepted_values(model=ref('model_one'), field='id', values=['a', 'b']) }}" # noqa
        relationships_sql = "{{ test_relationships(model=ref('model_one'), field='id', from='id', to=ref('model_two')) }}" # noqa

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
                    'fqn': ['root', 'schema_test', 'not_null_model_one_id'],
                    'empty': False,
                    'package_name': 'root',
                    'root_path': get_os_path('/usr/src/app'),
                    'refs': [('model_one',)],
                    'depends_on': {
                        'nodes': [],
                        'macros': ['macro.root.test_not_null']
                    },
                    'config': self.model_config,
                    'path': get_os_path(
                        'schema_test/not_null_model_one_id.sql'),
                    'tags': set(['schema']),
                    'raw_sql': not_null_sql,
                },
                'test.root.unique_model_one_id': {
                    'name': 'unique_model_one_id',
                    'resource_type': 'test',
                    'unique_id': 'test.root.unique_model_one_id',
                    'fqn': ['root', 'schema_test', 'unique_model_one_id'],
                    'empty': False,
                    'package_name': 'root',
                    'root_path': get_os_path('/usr/src/app'),
                    'refs': [('model_one',)],
                    'depends_on': {
                        'nodes': [],
                        'macros': ['macro.root.test_unique']
                    },
                    'config': self.model_config,
                    'path': get_os_path('schema_test/unique_model_one_id.sql'),
                    'tags': set(['schema']),
                    'raw_sql': unique_sql,
                },
                'test.root.accepted_values_model_one_id__a__b': {
                    'name': 'accepted_values_model_one_id__a__b',
                    'resource_type': 'test',
                    'unique_id': 'test.root.accepted_values_model_one_id__a__b', # noqa
                    'fqn': ['root', 'schema_test',
                            'accepted_values_model_one_id__a__b'],
                    'empty': False,
                    'package_name': 'root',
                    'root_path': get_os_path('/usr/src/app'),
                    'refs': [('model_one',)],
                    'depends_on': {
                        'nodes': [],
                        'macros': ['macro.root.test_accepted_values']
                    },
                    'config': self.model_config,
                    'path': get_os_path(
                        'schema_test/accepted_values_model_one_id__a__b.sql'),
                    'tags': set(['schema']),
                    'raw_sql': accepted_values_sql,
                },
                'test.root.relationships_model_one_id__id__ref_model_two_': {
                    'name': 'relationships_model_one_id__id__ref_model_two_',
                    'resource_type': 'test',
                    'unique_id': 'test.root.relationships_model_one_id__id__ref_model_two_', # noqa
                    'fqn': ['root', 'schema_test',
                            'relationships_model_one_id__id__ref_model_two_'],
                    'empty': False,
                    'package_name': 'root',
                    'root_path': get_os_path('/usr/src/app'),
                    'refs': [('model_one',), ('model_two',)],
                    'depends_on': {
                        'nodes': [],
                        'macros': ['macro.root.test_relationships']
                    },
                    'config': self.model_config,
                    'path': get_os_path('schema_test/relationships_model_one_id__id__ref_model_two_.sql'), # noqa
                    'tags': set(['schema']),
                    'raw_sql': relationships_sql,
                }


            }
        )

    def test__schema_test_with_comments(self):
        tests = [{
            'name': 'commented_test',
            'resource_type': 'test',
            'package_name': 'root',
            'root_path': get_os_path('/usr/src/app'),
            'path': 'commented_test.yml',
            'raw_sql': None,
            'raw_yml': '''
model:
    constraints:
        relationships:
#            - {from: customer_id, to: accounts, field: id}

another_model:
    constraints:
#       unique:
#            - id
'''
        }]

        self.assertEquals(
            dbt.parser.parse_schema_tests(
                tests,
                self.root_project_config,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {})

    def test__empty_schema_test(self):
        tests = [{
            'name': 'commented_test',
            'resource_type': 'test',
            'package_name': 'root',
            'root_path': get_os_path('/usr/src/app'),
            'path': 'commented_test.yml',
            'raw_sql': None,
            'raw_yml': ''
        }]

        self.assertEquals(
            dbt.parser.parse_schema_tests(
                tests,
                self.root_project_config,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {})

    def test__simple_data_test(self):
        tests = [{
            'name': 'no_events',
            'resource_type': 'test',
            'package_name': 'root',
            'path': 'no_events.sql',
            'root_path': get_os_path('/usr/src/app'),
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
                    'refs': [('base',)],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'config': self.model_config,
                    'path': 'no_events.sql',
                    'root_path': get_os_path('/usr/src/app'),
                    'tags': set(),
                    'raw_sql': self.find_input_by_name(
                        tests, 'no_events').get('raw_sql')
                }
            }
        )

    def test__simple_macro(self):
        macro_file_contents = """
{% macro simple(a, b) %}
  {{a}} + {{b}}
{% endmacro %}
"""

        result = dbt.parser.parse_macro_file(
            macro_file_path='simple_macro.sql',
            macro_file_contents=macro_file_contents,
            root_path=get_os_path('/usr/src/app'),
            package_name='root')

        self.assertEquals(
            type(result.get('macro.root.simple', {}).get('parsed_macro')),
            jinja2.runtime.Macro)

        del result['macro.root.simple']['parsed_macro']

        self.assertEquals(
            result,
            {
                'macro.root.simple': {
                    'name': 'simple',
                    'resource_type': 'macro',
                    'unique_id': 'macro.root.simple',
                    'package_name': 'root',
                    'depends_on': {
                        'macros': []
                    },
                    'root_path': get_os_path('/usr/src/app'),
                    'tags': set(),
                    'path': 'simple_macro.sql',
                    'raw_sql': macro_file_contents,
                }
            }
        )

    def test__macro_with_ref__invalid(self):
        macro_file_contents = """
{% macro with_ref(a) -%}
  {% if a: %}
    {{ ref(a) }}
  {% endif %}
{%- endmacro %}
"""

        with self.assertRaises(dbt.exceptions.CompilationException):
            dbt.parser.parse_macro_file(
                macro_file_path='macro_with_ref.sql',
                macro_file_contents=macro_file_contents,
                root_path=get_os_path('/usr/src/app'),
                package_name='root')

    def test__macro_with_var__invalid(self):
        macro_file_contents = """
{% macro with_var(a) -%}
  {% if a: %}
    {{ var('abc') }}
  {% endif %}
{%- endmacro %}
"""

        with self.assertRaises(dbt.exceptions.CompilationException):
            dbt.parser.parse_macro_file(
                macro_file_path='macro_with_var.sql',
                macro_file_contents=macro_file_contents,
                root_path=get_os_path('/usr/src/app'),
                package_name='root')

    def test__simple_macro_used_in_model(self):
        macro_file_contents = """
{% macro simple(a, b) %}
  {{a}} + {{b}}
{% endmacro %}
"""

        result = dbt.parser.parse_macro_file(
            macro_file_path='simple_macro.sql',
            macro_file_contents=macro_file_contents,
            root_path=get_os_path('/usr/src/app'),
            package_name='root')

        self.assertEquals(
            type(result.get('macro.root.simple', {}).get('parsed_macro')),
            jinja2.runtime.Macro)

        del result['macro.root.simple']['parsed_macro']

        self.assertEquals(
            result,
            {
                'macro.root.simple': {
                    'name': 'simple',
                    'resource_type': 'macro',
                    'unique_id': 'macro.root.simple',
                    'package_name': 'root',
                    'depends_on': {
                        'macros': []
                    },
                    'root_path': get_os_path('/usr/src/app'),
                    'tags': set(),
                    'path': 'simple_macro.sql',
                    'raw_sql': macro_file_contents,
                }
            }
        )

        models = [{
            'name': 'model_one',
            'resource_type': 'model',
            'package_name': 'root',
            'root_path': get_os_path('/usr/src/app'),
            'path': 'model_one.sql',
            'raw_sql': ("select *, {{package.simple(1, 2)}} from events"),
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
                    'root_path': get_os_path('/usr/src/app'),
                    'refs': [],
                    'depends_on': {
                        'nodes': [],
                        'macros': [
                            'macro.package.simple'
                        ]
                    },
                    'config': self.model_config,
                    'tags': set(),
                    'path': 'model_one.sql',
                    'raw_sql': self.find_input_by_name(
                        models, 'model_one').get('raw_sql')
                }
            }
        )

    def test__macro_no_explicit_project_used_in_model(self):
        models = [{
            'name': 'model_one',
            'resource_type': 'model',
            'package_name': 'root',
            'root_path': get_os_path('/usr/src/app'),
            'path': 'model_one.sql',
            'raw_sql': ("select *, {{ simple(1, 2) }} from events"),
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
                    'root_path': get_os_path('/usr/src/app'),
                    'refs': [],
                    'depends_on': {
                        'nodes': [],
                        'macros': [
                            'macro.root.simple'
                        ]
                    },
                    'config': self.model_config,
                    'tags': set(),
                    'path': 'model_one.sql',
                    'raw_sql': self.find_input_by_name(
                        models, 'model_one').get('raw_sql')
                }
            }
        )
