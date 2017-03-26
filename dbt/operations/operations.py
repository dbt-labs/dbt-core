
import jinja2.nodes
import jinja2.ext


class OperationExtension(jinja2.ext.Extension):
    tags = set(['op'])

    def __init__(self, environment):
        super(OperationExtension, self).__init__(environment)

    def parse(self, parser):
        lineno = next(parser.stream).lineno
        args = [parser.parse_expression()]
        body = parser.parse_statements(['name:endop'], drop_needle=True)

        return jinja2.nodes.CallBlock(
            self.call_method('_operation', args),
            [],
            [],
            body).set_lineno(lineno)

    def _operation(self, operation, caller):
        return operation(caller())


already_exists_sql = """
{{% if {not_token} already_exists('{schema}', '{table}') %}}
{contents}
{{% endif %}}
"""


def if_already_exists(model, does_exist=True):
    def render(contents):
        return already_exists_sql.format(
                schema=model.schema,
                table=model.name,
                contents=contents,
                not_token='not' if not does_exist else '')
    return render
