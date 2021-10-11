#!/usr/bin/env python
import argparse
import sys
from pathlib import Path

PROJECT_TEMPLATE = '''
name: dbt_{adapter}
version: {version}
config-version: 2

macro-paths: ["macros"]
'''


SETUP_PY_TEMPLATE = '''
#!/usr/bin/env python
from setuptools import find_namespace_packages, setup

package_name = "dbt-{adapter}"
# make sure this always matches dbt/adapters/{adapter}/__version__.py
package_version = "{version}"
description = """The {adapter} adapter plugin for dbt"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author={author_name},
    author_email={author_email},
    url={url},
    packages=find_namespace_packages(include=['dbt', 'dbt.*']),
    include_package_data=True,
    install_requires=[
        "{dbt_core_str}",{dependencies}
    ]
)
'''.lstrip()


MANIFEST_IN_TEMPLATE = "recursive-include dbt/include *.sql *.yml *.md"


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
from dataclasses import dataclass

from dbt.adapters.base import Credentials
from dbt.adapters.{adapter_src} import {connection_cls}


@dataclass
class {title_adapter}Credentials(Credentials):
    # Add credentials members here, like:
    # host: str
    # port: int
    # username: str
    # password: str

    @property
    def type(self):
        return '{adapter}'

    def _connection_keys(self):
        # return an iterator of keys to pretty-print in 'dbt debug'.
        # Omit fields like 'password'!
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


CATALOG_MACRO_TEMPLATE = """
{{% macro {adapter}__get_catalog(information_schema, schemas) -%}}

  {{% set msg -%}}
    get_catalog not implemented for {adapter}
  {{%- endset %}}

  {{{{ exceptions.raise_compiler_error(msg) }}}}
{{% endmacro %}}
"""


INCLUDE_INIT_TEXT = """
import os
PACKAGE_PATH = os.path.dirname(__file__)
""".lstrip()


SAMPLE_PROFILE_TEMPLATE = '''
default:
  outputs:
    dev:
      type: {adapter}
      # Add sample credentials here, like:
      # host: <host>
      # port: <port_num>
      # username: <user>
      # password: <pass>
  target: dev
'''


DBTSPEC_TEMPLATE = '''
# See https://github.com/dbt-labs/dbt-adapter-tests
# for installation and use

target:
  type: {adapter}
  # Add CI credentials here, like:
  # host: localhost
  # port: 5432
  # username: root
  # password: pass
sequences:
  test_dbt_empty: empty
  test_dbt_base: base
  test_dbt_ephemeral: ephemeral
  test_dbt_incremental: incremental
  test_dbt_snapshot_strategy_timestamp: snapshot_strategy_timestamp
  test_dbt_snapshot_strategy_check_cols: snapshot_strategy_check_cols
  test_dbt_data_test: data_test
  test_dbt_schema_test: schema_test
  test_dbt_ephemeral_data_tests: data_test_ephemeral_models
'''


class Builder:
    def __init__(self, args):
        self.args = args
        self.adapter = self.args.adapter
        self.dest = self.args.root / self.adapter
        # self.dbt_dir = self.dest / 'dbt'
        self.dbt_dir = Path('dbt')
        self.adapters = self.dbt_dir / 'adapters' / self.adapter
        self.include = self.dbt_dir / 'include' / self.adapter
        if self.dest.exists():
            raise Exception('path exists')

    def go(self):
        self.write_setup()
        self.write_adapter()
        self.write_include()
        self.write_test_spec()

    def include_paths(self):
        return [
            self.include / 'macros' / '*.sql',
            self.include / 'dbt_project.yml',
        ]

    def dest_path(self, *paths):
        return self.dest.joinpath(*paths)

    def write_setup(self):
        self.dest.mkdir(parents=True, exist_ok=True)

        dbt_core_str = 'dbt-core=={}'.format(self.args.dbt_core_version)

        # 12-space indent, then single-quoted with a trailing comma. The path
        # should not be the actual path from the root but from the 'dbt' dir
        # (because this is in the 'dbt' package)
        package_data = '\n'.join(
            "{}'{!s}',".format(12*' ', p.relative_to(self.dbt_dir))
            for p in self.include_paths()
        )

        setup_py_contents = SETUP_PY_TEMPLATE.format(
            adapter=self.adapter,
            version=self.args.package_version,
            author_name=self.args.author,
            author_email=self.args.email,
            url=self.args.url,
            dbt_core_str=dbt_core_str,
            dependencies=self.args.dependency,
        )
        self.dest_path('setup.py').write_text(setup_py_contents)
        self.dest_path('MANIFEST.in').write_text(MANIFEST_IN_TEMPLATE)

    def _make_adapter_kwargs(self):
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
            'upper_adapter': self.adapter.upper(),
            'title_adapter': self.args.title_case,
            'adapter': self.adapter,
        })

        return kwargs

    def write_adapter(self):
        adapters_dest = self.dest_path(self.adapters)
        adapters_dest.mkdir(parents=True, exist_ok=True)

        kwargs = self._make_adapter_kwargs()

        init_text = ADAPTER_INIT_TEMPLATE.format(
            adapter=self.adapter,
            title_adapter=self.args.title_case
        )
        version_text = f'{self.args.package_version}'
        connections_text = ADAPTER_CONNECTIONS_TEMPLATE.format(**kwargs)
        impl_text = ADAPTER_IMPL_TEMPLATE.format(**kwargs)

        (adapters_dest / '__init__.py').write_text(init_text)
        (adapters_dest / '__version__.py').write_text(version_text)
        (adapters_dest / 'connections.py').write_text(connections_text)
        (adapters_dest / 'impl.py').write_text(impl_text)

    def write_include(self):
        include_dest = self.dest_path(self.include)
        include_dest.mkdir(parents=True, exist_ok=True)
        macros_dest = include_dest / 'macros'
        macros_dest.mkdir(exist_ok=True)

        dbt_project_text = PROJECT_TEMPLATE.format(
            adapter=self.adapter,
            version=self.args.project_version,
        )
        sample_profiles_text = SAMPLE_PROFILE_TEMPLATE.format(
            adapter=self.adapter
        )
        catalog_macro_text = CATALOG_MACRO_TEMPLATE.format(
            adapter=self.adapter
        )

        (include_dest / '__init__.py').write_text(INCLUDE_INIT_TEXT)
        (include_dest / 'dbt_project.yml').write_text(dbt_project_text)
        (include_dest / 'sample_profiles.yml').write_text(sample_profiles_text)
        # make sure something satisfies the 'include/macros/*.sql' in setup.py
        (macros_dest / 'catalog.sql').write_text(catalog_macro_text)

    def write_test_spec(self):
        test_dest = self.dest_path('test')
        test_dest.mkdir(parents=True, exist_ok=True)
        spec_file = f'{self.adapter}.dbtspec'
        spec_text = DBTSPEC_TEMPLATE.format(adapter=self.adapter)
        (test_dest / spec_file).write_text(spec_text)


def parse_args(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument('root', type=Path)
    parser.add_argument('adapter')
    parser.add_argument('--title-case', '-t', default=None)
    parser.add_argument('--dependency', action='append')
    parser.add_argument('--dbt-core-version', default='1.0.0b1')
    parser.add_argument('--email')
    parser.add_argument('--author')
    parser.add_argument('--url')
    parser.add_argument('--sql', action='store_true')
    parser.add_argument('--package-version', default='1.0.0b1')
    parser.add_argument('--project-version', default='1.0')
    parser.add_argument(
        '--no-dependency', action='store_false', dest='set_dependency'
    )
    parsed = parser.parse_args()

    if parsed.title_case is None:
        parsed.title_case = parsed.adapter.title()

    if parsed.set_dependency:
        
        prefix = '\n        '
        
        if parsed.dependency:
            # ['a', 'b'] => "'a',\n        'b'"; ['a'] -> "'a',"
            
            parsed.dependency = prefix + prefix.join(
                "'{}',".format(d) for d in parsed.dependency
            )
        else:
            parsed.dependency = prefix + '<INSERT DEPENDENCIES HERE>'
    else:
        parsed.dependency = ''

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
