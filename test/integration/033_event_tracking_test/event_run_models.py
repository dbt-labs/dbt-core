
def expected(user_id, invocation_id, project_id, version):
    return [
        [
            (),
            {
                'category': 'dbt',
                'action': 'invocation',
                'label': 'start',
                'context': [
                    {
                        'schema': 'iglu:com.dbt/invocation/jsonschema/1-0-0',
                        'data': {
                            'project_id': project_id,
                            'user_id': user_id,
                            'invocation_id': invocation_id,
                            'version': version,

                            'run_type': 'regular',
                            'progress': 'start',
                            'command': 'run',

                            'options': None,
                            'result_type': None,
                            'result': None
                        }
                    },
                    {
                        'schema': 'iglu:com.dbt/platform/jsonschema/1-0-0',
                        'data': {
                            'platform': 'Linux-4.9.87-linuxkit-aufs-x86_64-with-debian-8.10',
                            'python': '3.6.5',
                            'python_version': 'CPython'
                        }
                    },
                    {
                        'schema': 'iglu:com.dbt/invocation_env/jsonschema/1-0-0',
                        'data': {
                            'environment': 'development'
                        }
                    }
                ]
            }
        ],
        [
            (),
            {
                'category': 'dbt',
                'action': 'run_model',
                'label': invocation_id,
                'context': [
                    {
                        'schema': 'iglu:com.dbt/run_model/jsonschema/1-0-0',
                        'data': {
                            'invocation_id': invocation_id,
                            'execution_time': 0.048723459243774414,

                            'model_materialization': 'view',
                            'model_id': '4fbacae0e1b69924b22964b457148fb8',
                            'hashed_contents': '1e5789d34cddfbd5da47d7713aa9191c',

                            'run_status': 'CREATE VIEW',
                            'run_skipped': False,
                            'run_error': None,
                            'index': 1,
                            'total': 2,
                        }
                    }
                ]
            }
        ],
        [
            (),
            {
                'category': 'dbt',
                'action': 'run_model',
                'label': invocation_id,
                'context': [
                    {
                        'schema': 'iglu:com.dbt/run_model/jsonschema/1-0-0',
                        'data': {
                            'invocation_id': invocation_id,
                            'execution_time': 0.04148578643798828,

                            'model_materialization': 'view',
                            'model_id': '57994a805249953b31b738b1af7a1eeb',
                            'hashed_contents': '20ff78afb16c8b3b8f83861b1d3b99bd',

                            'run_status': 'CREATE VIEW',
                            'run_skipped': False,
                            'run_error': None,
                            'index': 2,
                            'total': 2,
                        }
                    }
                ]
            }
        ],
        [
            (),
            {
                'category': 'dbt',
                'action': 'invocation',
                'label': 'end',
                'context': [
                    {
                        'schema': 'iglu:com.dbt/invocation/jsonschema/1-0-0',
                        'data': {
                            'project_id': project_id,
                            'user_id': user_id,
                            'invocation_id': invocation_id,
                            'version': version,

                            'command': 'run',
                            'run_type': 'regular',
                            'progress': 'end',
                            'result_type': 'ok',

                            'options': None,
                            'result': None
                        }
                    },
                    {
                        'schema': 'iglu:com.dbt/platform/jsonschema/1-0-0',
                        'data': {
                            'platform': 'Linux-4.9.87-linuxkit-aufs-x86_64-with-debian-8.10',
                            'python': '3.6.5',
                            'python_version': 'CPython'
                        }
                    },
                    {
                        'schema': 'iglu:com.dbt/invocation_env/jsonschema/1-0-0',
                        'data': {
                            'environment': 'development'
                        }
                    }
                ]
            }
        ]
    ]

# null out the execution time, as it is nondeterministic
def transform(events):
    for event in events:
        args, kwargs = event
        if kwargs['action'] == 'run_model':
            for context in kwargs['context']:
                if context['schema'] == 'iglu:com.dbt/run_model/jsonschema/1-0-0':
                    context['data']['execution_time'] = None
    return events
