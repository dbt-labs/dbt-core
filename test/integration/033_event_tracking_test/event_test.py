
##############################################################################
# TODO TODO TODO        We are not tracking test events!        TODO TODO TODO
##############################################################################

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

                            'progress': 'start',
                            'command': 'test',
                            'run_type': 'regular',

                            'result_type': None,
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

                            'command': 'test',
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
    return events
