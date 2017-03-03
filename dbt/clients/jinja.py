import dbt.compat
import dbt.exceptions

import jinja2
import jinja2.sandbox


class SilentUndefined(jinja2.Undefined):
    """
    This class sets up the parser to just ignore undefined jinja2 calls. So,
    for example, `env` is not defined here, but will not make the parser fail
    with a fatal error.
    """
    def _fail_with_undefined_error(self, *args, **kwargs):
        return None

    __add__ = __radd__ = __mul__ = __rmul__ = __div__ = __rdiv__ = \
        __truediv__ = __rtruediv__ = __floordiv__ = __rfloordiv__ = \
        __mod__ = __rmod__ = __pos__ = __neg__ = __call__ = \
        __getitem__ = __lt__ = __le__ = __gt__ = __ge__ = __int__ = \
        __float__ = __complex__ = __pow__ = __rpow__ = \
        _fail_with_undefined_error


env = jinja2.sandbox.SandboxedEnvironment()

silent_on_undefined_env = jinja2.sandbox.SandboxedEnvironment(
    undefined=SilentUndefined)


def get_template(string, ctx, node=None, silent_on_undefined=False):
    try:
        local_env = env

        if silent_on_undefined:
            local_env = silent_on_undefined_env

        return local_env.from_string(dbt.compat.to_string(string), globals=ctx)

    except (jinja2.exceptions.TemplateSyntaxError,
            jinja2.exceptions.UndefinedError) as e:
        dbt.exceptions.raise_compiler_error(node, str(e))


def get_rendered(string, ctx, node=None, silent_on_undefined=False):
    try:
        template = get_template(string, ctx, node, silent_on_undefined)
        return template.render(ctx)

    except (jinja2.exceptions.TemplateSyntaxError,
            jinja2.exceptions.UndefinedError) as e:
        dbt.exceptions.raise_compiler_error(node, str(e))
