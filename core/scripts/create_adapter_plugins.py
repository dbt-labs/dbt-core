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
    long_description_content_type=description,
    author={author_name},
    author_email={author_email},
    url={url},
    packages=find_packages(),
    install_requires=[
        'dbt-core=={dbt_core_version}',
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


INCLUDE_INIT_TEMPLATE = '''
import os
PACKAGE_PATH = os.path.dirname(os.path.dirname(__file__))
'''.lstrip()


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
    parsed = parse_args()
    dest = pj(parsed.root, parsed.adapter)
    if os.path.exists(dest):
        raise Exception('path exists')

    adapters_path = pj(dest, 'dbt', 'adapters', parsed.adapter)
    include_path = pj(dest, 'dbt', 'include', parsed.adapter)
    os.makedirs(adapters_path)
    os.makedirs(pj(include_path, 'macros'))

    # namespaces!
    with open(pj(dest, 'dbt', '__init__.py'), 'w') as fp:
        fp.write(NAMESPACE_INIT_TEMPLATE)
    with open(pj(dest, 'dbt', 'adapters', '__init__.py'), 'w') as fp:
        fp.write(NAMESPACE_INIT_TEMPLATE)
    with open(pj(dest, 'dbt', 'include', '__init__.py'), 'w') as fp:
        fp.write(NAMESPACE_INIT_TEMPLATE)

    # setup script!
    with open(pj(dest, 'setup.py'), 'w') as fp:
        fp.write(SETUP_PY_TEMPLATE.format(adapter=parsed.adapter,
                                          version=parsed.package_version,
                                          author_name=parsed.author,
                                          author_email=parsed.email,
                                          url=parsed.url,
                                          dbt_core_version=parsed.dbt_core_version,
                                          dependencies=parsed.dependency))


    # adapter stuff!
    with open(pj(adapters_path, '__init__.py'), 'w') as fp:
        fp.write(ADAPTER_INIT_TEMPLATE.format(adapter=parsed.adapter,
                                              title_adapter=parsed.title_case))

    # macro/project stuff!
    with open(pj(include_path, '__init__.py'), 'w') as fp:
        fp.write(INCLUDE_INIT_TEMPLATE)

    with open(pj(include_path, 'dbt_project.yml'), 'w') as fp:
        fp.write(PROJECT_TEMPLATE.format(adapter=parsed.adapter,
                                         version=parsed.project_version))

    # TODO:
    # - bare class impls for mandatory subclasses
    #       (ConnectionManager, Credentials, Adapter)
    # - impls of mandatory abstract methods w/explicit NotImplementedErrors


if __name__ == '__main__':
    main()
