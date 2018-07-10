
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
                            'invocation_id': invocation_id,
                            'user_id': user_id,
                            'version': version,
                            'project_id': project_id,

                            'command': 'seed',
                            'progress': 'start',
                            'run_type': 'regular',

                            'options': None,
                            'result': None,
                            'result_type': None,
                        },
                    },
                    {
                        'schema': 'iglu:com.dbt/platform/jsonschema/1-0-0',
                        'data': {
                            'platform': 'Linux-4.9.87-linuxkit-aufs-x86_64-with-debian-8.10',
                            'python': '3.6.5',
                            'python_version': 'CPython'
                        },
                    },
                    {
                        'schema': 'iglu:com.dbt/invocation_env/jsonschema/1-0-0',
                        'data': {
                            'environment': 'development'
                        },
                    }
                ],
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

                            'model_materialization': 'seed',

                            'execution_time': 0.04340934753417969,
                            'hashed_contents': '4f67ae18b42bc9468cc95ca0dab30531',
                            'model_id': '39bc2cd707d99bd3e600d2faaafad7ae',

                            'index': 1,
                            'total': 1,

                            'run_status': 'INSERT 1',
                            'run_error': None,
                            'run_skipped': False,
                        },
                    }
                ],
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
                            'invocation_id': invocation_id,
                            'user_id': user_id,
                            'project_id': project_id,
                            'version': version,

                            'command': 'seed',
                            'progress': 'end',
                            'result_type': 'ok',
                            'run_type': 'regular',

                            'options': None,
                            'result': None,
                        },
                    },
                    {
                        'schema': 'iglu:com.dbt/platform/jsonschema/1-0-0',
                        'data': {
                            'platform': 'Linux-4.9.87-linuxkit-aufs-x86_64-with-debian-8.10',
                            'python': '3.6.5',
                            'python_version': 'CPython'
                        },
                    },
                    {
                        'schema': 'iglu:com.dbt/invocation_env/jsonschema/1-0-0',
                        'data': {
                            'environment': 'development'
                        },
                    }
                ],
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
