#!/usr/bin/env python
import argparse
import os
import sys

pj = os.path.join

PROJECT_TEMPLATE = '''
name: dbt_{adapter}
version: {version}

macro-paths: ["macros"]
'''

NAMESPACE_INIT_TEMPLATE = '''
__path__ = __import__('pkgutil').extend_path(__path__, __name__)
'''.lstrip()


# TODO: make this not default to fishtown for everything!
SETUP_PY_TEMPLATE = '''
#!/usr/bin/env python
from setuptools import find_packages
from distutils.core import setup

package_name = "dbt-{adapter}"
package_version = "{version}"
description = """The {adapter} adpter plugin for dbt (data build tool)"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author={author_name},
    author_email={author_email},
    url={url},
    packages=find_packages(),
    package_data={{
        'dbt': [
            'include/{adapter}/dbt_project.yml',
            'include/{adapter}/macros/*.sql',
        ]
    }},
    install_requires=[
        {dbt_core_str},
        {dependencies}
    ]
)
'''.lstrip()



ADAPTER_INIT_TEMPLATE = '''
from dbt.adapters.{adapter}.connections import {title_adapter}ConnectionManager
from dbt.adapters.{adapter}.connections import {title_adapter}Credentials
from dbt.adapters.{adapter}.impl import {title_adapter}Adapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import {adapter}


Plugin = AdapterPlugin(
    adapter={title_adapter}Adapter,
    credentials={title_adapter}Credentials,
    include_path={adapter}.PACKAGE_PATH)
'''.lstrip()



ADAPTER_CONNECTIONS_TEMPLATE = '''
from contextlib import contextmanager

from dbt.adapters.base import Credentials
from dbt.adapters.{adapter_src} import {connection_cls}


{upper_adapter}_CREDENTIALS_CONTRACT = {{
    'type': 'object',
    'additionalProperties': False,
    'properties': {{
        'database': {{
            'type': 'string',
        }},
        'schema': {{
            'type': 'string',
        }},
    }},
    'required': ['database', 'schema'],
}}


class {title_adapter}Credentials(Credentials):
    SCHEMA = {upper_adapter}_CREDENTIALS_CONTRACT

    @property
    def type(self):
        return '{adapter}'

    def _connection_keys(self):
        # return an iterator of keys to pretty-print in 'dbt debug'
        raise NotImplementedError


class {title_adapter}ConnectionManager({connection_cls}):
    TYPE = '{adapter}'
'''.lstrip()


ADAPTER_IMPL_TEMPLATE = '''
from dbt.adapters.{adapter_src} import {adapter_cls}
from dbt.adapters.{adapter} import {title_adapter}ConnectionManager


class {title_adapter}Adapter({adapter_cls}):
    ConnectionManager = {title_adapter}ConnectionManager
'''.lstrip()


INCLUDE_INIT_TEMPLATE = '''
import os
PACKAGE_PATH = os.path.dirname(__file__)
'''.lstrip()


class Builder:
    def __init__(self, args):
        self.args = args
        self.dest = pj(self.args.root, self.args.adapter)
        self.dbt_dir = pj(self.dest, 'dbt')
        self.adapters_path = pj(self.dbt_dir, 'adapters', self.args.adapter)
        self.include_path = pj(self.dbt_dir, 'include', self.args.adapter)
        if os.path.exists(self.dest):
            raise Exception('path exists')

    def go(self):
        self.build_namespace()
        self.write_setup()
        self.write_include()
        self.write_adapter()

    def build_namespace(self):
        """Build out the directory skeleton and python namespace files:

            dbt/
                __init__.py
                adapters/
                    ${adapter_name}
                    __init__.py
                include/
                    ${adapter_name}
                    __init__.py
        """
        os.makedirs(self.adapters_path)
        os.makedirs(pj(self.include_path, 'macros'))
        with open(pj(self.dbt_dir, '__init__.py'), 'w') as fp:
            fp.write(NAMESPACE_INIT_TEMPLATE)
        with open(pj(self.dbt_dir, 'adapters', '__init__.py'), 'w') as fp:
            fp.write(NAMESPACE_INIT_TEMPLATE)
        with open(pj(self.dbt_dir, 'include', '__init__.py'), 'w') as fp:
            fp.write(NAMESPACE_INIT_TEMPLATE)

    def write_setup(self):
        if self.args.dbt_core_version == self.args.package_version:
            dbt_core_str = "'dbt-core=={}'.format(package_version)"
        else:
            dbt_core_str = 'dbt-core=={}'.format(self.args.dbt_core_version)
        setup_py_contents = SETUP_PY_TEMPLATE.format(
            adapter=self.args.adapter,
            version=self.args.package_version,
            author_name=self.args.author,
            author_email=self.args.email,
            url=self.args.url,
            dbt_core_str=dbt_core_str,
            dependencies=self.args.dependency
        )
        with open(pj(self.dest, 'setup.py'), 'w') as fp:
            fp.write(setup_py_contents)

    def write_adapter(self):
        adapter_init_contents = ADAPTER_INIT_TEMPLATE.format(
            adapter=self.args.adapter,
            title_adapter=self.args.title_case
        )
        with open(pj(self.adapters_path, '__init__.py'), 'w') as fp:
            fp.write(adapter_init_contents)

        if self.args.sql:
            kwargs = {
                'adapter_src': 'sql',
                'adapter_cls': 'SQLAdapter',
                'connection_cls': 'SQLConnectionManager',
            }
        else:
            kwargs = {
                'adapter_src': 'base',
                'adapter_cls': 'BaseAdapter',
                'connection_cls': 'BaseConnectionManager',
            }
        kwargs.update({
            'upper_adapter': self.args.adapter.upper(),
            'title_adapter': self.args.title_case,
            'adapter': self.args.adapter,
        })

        adapter_connections_contents = ADAPTER_CONNECTIONS_TEMPLATE.format(
            **kwargs
        )
        with open(pj(self.adapters_path, 'connections.py'), 'w') as fp:
            fp.write(adapter_connections_contents)

        adapter_impl_contents = ADAPTER_IMPL_TEMPLATE.format(
            **kwargs
        )
        with open(pj(self.adapters_path, 'impl.py'), 'w') as fp:
            fp.write(adapter_impl_contents)

    def write_include(self):
        with open(pj(self.include_path, '__init__.py'), 'w') as fp:
            fp.write(INCLUDE_INIT_TEMPLATE)

        with open(pj(self.include_path, 'dbt_project.yml'), 'w') as fp:
            fp.write(PROJECT_TEMPLATE.format(adapter=self.args.adapter,
                                             version=self.args.project_version))

def parse_args(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument('root')
    parser.add_argument('adapter')
    parser.add_argument('--title-case', '-t', default=None)
    parser.add_argument('--dependency', action='append')
    parser.add_argument('--dbt-core-version', default='0.13.0')
    parser.add_argument('--email')
    parser.add_argument('--author')
    parser.add_argument('--url')
    parser.add_argument('--sql', action='store_true')
    parser.add_argument('--package-version', default='0.0.1')
    parser.add_argument('--project-version', default='1.0')
    parsed = parser.parse_args()

    if parsed.title_case is None:
        parsed.title_case = parsed.adapter.title()

    if parsed.dependency:
        #['a', 'b'] => "'a',\n        'b'"; ['a'] -> "'a',"
        parsed.dependency = '\n        '.join(
            "'{}',".format(d) for d in parsed.dependency
        )
    else:
        parsed.dependency = '<INSERT DEPENDENCIES HERE>'

    if parsed.email is not None:
        parsed.email = "'{}'".format(parsed.email)
    else:
        parsed.email = '<INSERT EMAIL HERE>'
    if parsed.author is not None:
        parsed.author = "'{}'".format(parsed.author)
    else:
        parsed.author = '<INSERT AUTHOR HERE>'
    if parsed.url is not None:
        parsed.url = "'{}'".format(parsed.url)
    else:
        parsed.url = '<INSERT URL HERE>'
    return parsed



def main():
    builder = Builder(parse_args())
    builder.go()


if __name__ == '__main__':
    main()
