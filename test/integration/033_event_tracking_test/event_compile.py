

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

                            'command': 'compile',
                            'progress': 'start',
                            'run_type': 'regular',

                            'options': None, # TODO : Add options to compile cmd!
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

                            'command': 'compile',
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

# Nothing to process here
def transform(events):
    return events
