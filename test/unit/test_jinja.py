import unittest

from dbt.clients.jinja import get_template
from dbt.clients.jinja import extract_toplevel_blocks
from dbt.exceptions import CompilationException


class TestJinja(unittest.TestCase):
    def test_do(self):
        s = '{% set my_dict = {} %}\n{% do my_dict.update(a=1) %}'

        template = get_template(s, {})
        mod = template.make_module()
        self.assertEqual(mod.my_dict, {'a': 1})


class TestBlockLexer(unittest.TestCase):
    def test_basic(self):
        body = '{{ config(foo="bar") }}\r\nselect * from this.that\r\n'
        block_data = '  \n\r\t{%- mytype foo %}'+body+'{%endmytype -%}'
        blocks = extract_toplevel_blocks(block_data)
        self.assertEqual(len(blocks), 1)
        self.assertEqual(blocks[0].block_type_name, 'mytype')
        self.assertEqual(blocks[0].block_name, 'foo')
        self.assertEqual(blocks[0].contents, body)
        self.assertEqual(blocks[0].full_block, block_data)

    def test_multiple(self):
        body_one = '{{ config(foo="bar") }}\r\nselect * from this.that\r\n'
        body_two = (
            '{{ config(bar=1)}}\r\nselect * from {% if foo %} thing '
            '{% else %} other_thing {% endif %}'
        )

        block_data = (
            '  {% mytype foo %}' + body_one + '{% endmytype %}' +
            '\r\n{% othertype bar %}' + body_two + '{% endothertype %}'
        )
        blocks = extract_toplevel_blocks(block_data)
        self.assertEqual(len(blocks), 2)

    def test_comments(self):
        body = '{{ config(foo="bar") }}\r\nselect * from this.that\r\n'
        comment = '{# my comment #}'
        block_data = '  \n\r\t{%- mytype foo %}'+body+'{%endmytype -%}'
        blocks = extract_toplevel_blocks(comment+block_data)
        self.assertEqual(len(blocks), 1)
        self.assertEqual(blocks[0].block_type_name, 'mytype')
        self.assertEqual(blocks[0].block_name, 'foo')
        self.assertEqual(blocks[0].contents, body)
        self.assertEqual(blocks[0].full_block, block_data)

    def test_evil_comments(self):
        body = '{{ config(foo="bar") }}\r\nselect * from this.that\r\n'
        comment = '{# external comment {% othertype bar %} select * from thing.other_thing{% endothertype %} #}'
        block_data = '  \n\r\t{%- mytype foo %}'+body+'{%endmytype -%}'
        blocks = extract_toplevel_blocks(comment+block_data)
        self.assertEqual(len(blocks), 1)
        self.assertEqual(blocks[0].block_type_name, 'mytype')
        self.assertEqual(blocks[0].block_name, 'foo')
        self.assertEqual(blocks[0].contents, body)
        self.assertEqual(blocks[0].full_block, block_data)

    def test_nested_comments(self):
        body = '{# my comment #} {{ config(foo="bar") }}\r\nselect * from {# my other comment embedding {% endmytype %} #} this.that\r\n'
        block_data = '  \n\r\t{%- mytype foo %}'+body+'{% endmytype -%}'
        comment = '{# external comment {% othertype bar %} select * from thing.other_thing{% endothertype %} #}'
        blocks = extract_toplevel_blocks(comment+block_data)
        self.assertEqual(len(blocks), 1)
        self.assertEqual(blocks[0].block_type_name, 'mytype')
        self.assertEqual(blocks[0].block_name, 'foo')
        self.assertEqual(blocks[0].contents, body)
        self.assertEqual(blocks[0].full_block, block_data)

    def test_complex_file(self):
        blocks = extract_toplevel_blocks(complex_archive_file)
        self.assertEqual(len(blocks), 3)
        self.assertEqual(blocks[0].block_type_name, 'mytype')
        self.assertEqual(blocks[0].block_name, 'foo')
        self.assertEqual(blocks[0].full_block, '{% mytype foo %} some stuff {% endmytype %}')
        self.assertEqual(blocks[0].contents, ' some stuff ')
        self.assertEqual(blocks[1].block_type_name, 'mytype')
        self.assertEqual(blocks[1].block_name, 'bar')
        self.assertEqual(blocks[1].full_block, bar_block)
        self.assertEqual(blocks[1].contents, bar_block[16:-15].rstrip())
        self.assertEqual(blocks[2].block_type_name, 'myothertype')
        self.assertEqual(blocks[2].block_name, 'x')
        self.assertEqual(blocks[2].full_block, x_block.strip())
        self.assertEqual(blocks[2].contents, x_block[len('\n{% myothertype x %}'):-len('{% endmyothertype %}\n')])

    def test_peaceful_macro_coexistence(self):
        body = '{# my macro #} {% macro foo(a, b) %} do a thing {%- endmacro %} {# my model #} {% a b %} {% enda %}'
        blocks = extract_toplevel_blocks(body)
        self.assertEqual(len(blocks), 2)
        self.assertEqual(blocks[0].block_type_name, 'macro')
        self.assertEqual(blocks[0].block_name, 'foo')
        self.assertEqual(blocks[0].contents, ' do a thing')
        self.assertEqual(blocks[1].block_type_name, 'a')
        self.assertEqual(blocks[1].block_name, 'b')
        self.assertEqual(blocks[1].contents, ' ')

    def test_macro_with_crazy_args(self):
        body = '''{% macro foo(a, b=asdf("cool this is 'embedded'" * 3) + external_var, c)%}cool{# block comment with {% endmacro %} in it #} stuff here {% endmacro %}'''
        blocks = extract_toplevel_blocks(body)
        self.assertEqual(len(blocks), 1)
        self.assertEqual(blocks[0].block_type_name, 'macro')
        self.assertEqual(blocks[0].block_name, 'foo')
        self.assertEqual(blocks[0].contents, 'cool{# block comment with {% endmacro %} in it #} stuff here ')

    def test_materialization_parse(self):
        body = '{% materialization xxx, default %} ... {% endmaterialization %}'
        blocks = extract_toplevel_blocks(body)
        self.assertEqual(len(blocks), 1)
        self.assertEqual(blocks[0].block_type_name, 'materialization')
        self.assertEqual(blocks[0].block_name, 'xxx')
        self.assertEqual(blocks[0].full_block, body)

        body = '{% materialization xxx, adapter="other" %} ... {% endmaterialization %}'
        blocks = extract_toplevel_blocks(body)
        self.assertEqual(len(blocks), 1)
        self.assertEqual(blocks[0].block_type_name, 'materialization')
        self.assertEqual(blocks[0].block_name, 'xxx')
        self.assertEqual(blocks[0].full_block, body)

    def test_nested_failure(self):
        # we don't allow nesting same blocks
        # ideally we would not allow nesting any, but that's much harder
        body = '{% myblock a %} {% myblock b %} {% endmyblock %} {% endmyblock %}'
        with self.assertRaises(CompilationException):
            extract_toplevel_blocks(body)

    def test_incomplete_block_failure(self):
        fullbody = '{% myblock foo %} {% endblock %}'
        for length in range(1, len(fullbody)-1):
            body = fullbody[:length]
        with self.assertRaises(CompilationException):
            extract_toplevel_blocks(body)

    def test_wrong_end_failure(self):
        body = '{% myblock foo %} {% endotherblock %}'
        with self.assertRaises(CompilationException):
            extract_toplevel_blocks(body)

    def test_comment_no_end_failure(self):
        body = '{# '
        with self.assertRaises(CompilationException):
            extract_toplevel_blocks(body)

    def test_comment_only(self):
        body = '{# myblock #}'
        blocks = extract_toplevel_blocks(body)
        self.assertEqual(len(blocks), 0)

bar_block = '''{% mytype bar %}
{# a comment
    that inside it has
    {% mytype baz %}
{% endmyothertype %}
{% endmytype %}
{% endmytype %}
    {#
{% endmytype %}#}

some other stuff

{%- endmytype%}'''

x_block = '''
{% myothertype x %}
before
{##}
and after
{% endmyothertype %}
'''

complex_archive_file = '''
{#some stuff {% mytype foo %} #}
{% mytype foo %} some stuff {% endmytype %}

'''+bar_block+x_block
