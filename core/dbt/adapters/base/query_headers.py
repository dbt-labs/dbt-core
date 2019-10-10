from threading import local
from typing import Optional, Callable

from dbt.clients.jinja import QueryStringGenerator

from dbt.contracts.connection import HasCredentials
# this generates an import cycle, as usual
from dbt.context.base import QueryHeaderContext
from dbt.contracts.graph.compiled import CompileResultNode


default_query_comment = '''
{%- set comment_dict = {} -%}
{%- do comment_dict.update(target) -%}
{%- do comment_dict.update(
    app='dbt',
    dbt_version=dbt_version,
) -%}
{%- if node is not none -%}
  {%- do comment_dict.update(
    file=node.original_file_path,
    node_id=node.unique_id,
    node_name=node.name,
    resource_type=node.resource_type,
    package_name=node.package_name,
    tags=node.tags,
    identifier=node.identifier,
    schema=node.schema,
    database=node.database,
  ) -%}
{% else %}
  {# in the node context, the connection name is the node_id #}
  {%- do comment_dict.update(connection_name=connection_name) -%}
{%- endif -%}
{{ return(tojson(comment_dict)) }}
'''


class NodeWrapper:
    def __init__(self, node):
        self._inner_node = node

    def __getattr__(self, name):
        return getattr(self._inner_node, name, '')


class _QueryComment(local):
    """A thread-local class storing thread-specific state information for
    connection management, namely:
        - the current thread's query comment.
        - a source_name indicating what set the current thread's query comment
    """
    def __init__(self, initial):
        self.query_comment: str = initial

    def add(self, sql: str) -> str:
        # Make sure there are no trailing newlines.
        # For every newline, add a comment after it in case query_comment
        # is multiple lines.
        # Then add a comment to the first line of the query comment, and
        # put the sql on a fresh line.
        comment_split = self.query_comment.strip().replace('\n', '\n-- ')
        return '-- {}\n{}'.format(comment_split, sql)

    def set(self, comment: str):
        self.query_comment = comment


QueryStringFunc = Callable[[str, Optional[CompileResultNode]], str]


class QueryStringSetter:
    def __init__(self, config: HasCredentials):
        if config.config.query_comment is not None:
            comment = config.config.query_comment
        else:
            comment = default_query_comment
        macro = '\n'.join((
            '{%- macro query_comment_macro(connection_name, node) -%}',
            comment,
            '{% endmacro %}'
        ))

        ctx = QueryHeaderContext(config).to_dict()
        self.generator: QueryStringFunc = QueryStringGenerator(macro, ctx)
        self.comment = _QueryComment('')
        self.reset()

    def add(self, sql: str) -> str:
        return self.comment.add(sql)

    def reset(self):
        self.set('master', None)

    def set(self, name: str, node: Optional[CompileResultNode]):
        if node is not None:
            wrapped = NodeWrapper(node)
        else:
            wrapped = None
        comment_str = self.generator(name, wrapped)
        self.comment.set(comment_str)
