
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

                            'command': 'run',
                            'run_type':'regular',
                            'progress': 'start',

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
            'context':
                [
                    {
                        'schema': 'iglu:com.dbt/run_model/jsonschema/1-0-0',
                        'data': {
                            'invocation_id': invocation_id,
                            'execution_time': 0.05639934539794922,

                            'model_materialization': 'view',
                            'model_id': '576c3d4489593f00fad42b97c278641e',
                            'hashed_contents': '4419e809ce0995d99026299e54266037',

                            'index': 1,
                            'total': 1,
                            'run_status': 'ERROR',
                            'run_skipped': False,
                            'run_error': None,
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
                            'progress': 'end',
                            'run_type': 'regular',
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
    import pdb; pdb.set_trace()
