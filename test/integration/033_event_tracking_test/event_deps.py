
def expected(user_id, invocation_id, project_id, version):
    return [
        [
            # ------------ EVENT 1 - Invocation -----------

            # args
            (),

            # kwargs
            {
                "category": "dbt",
                "action": "invocation",
                "label": "start",

                "context": [
                    {
                        'schema': 'iglu:com.dbt/invocation/jsonschema/1-0-0',
                        'data': {
                            'project_id': project_id,
                            'user_id': user_id,
                            'invocation_id': invocation_id,
                            'version': version,

                            'command': 'deps',
                            'progress': 'start',
                            'run_type': 'regular',

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
            },
        ],

            # ------------ EVENT 2 - Installation ------------
        [
            # args
            (),

            # kwargs
            {
                "category": "dbt",
                "action": "package",
                "label": invocation_id,
                "property_": "install",
                "context": [
                    {
                        'schema': 'iglu:com.dbt/package_install/jsonschema/1-0-0',
                        'data': {
                            'name': 'c5552991412d1cd86e5c20a87f3518d5',
                            'source': 'git',
                            'version': 'eb0a191797624dd3a48fa681d3061212'
                        }
                    }
                ]
            },
        ],
            # ------------ EVENT 3 - Run End ------------
        [

            # args
            (),

            # kwargs
            {
                "category": "dbt",
                "action": "invocation",
                "label": "end",
                "context": [
                    {
                        'schema': 'iglu:com.dbt/invocation/jsonschema/1-0-0',
                        'data': {
                            'project_id': project_id,
                            'user_id': user_id,
                            'invocation_id': invocation_id,
                            'version': version,

                            'command': 'deps',
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
            },
        ]
    ]

# Nothing to process here
def transform(events):
    return events
