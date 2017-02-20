import copy
import jinja2
import jinja2.sandbox
import os

import dbt.model
import dbt.utils


class SilentUndefined(jinja2.Undefined):
    """
    Don't fail to parse because of undefined things. This allows us to parse
    models before macros, since we aren't guaranteed to know about macros
    before models.
    """
    def _fail_with_undefined_error(self, *args, **kwargs):
        return None

    __add__ = __radd__ = __mul__ = __rmul__ = __div__ = __rdiv__ = \
        __truediv__ = __rtruediv__ = __floordiv__ = __rfloordiv__ = \
        __mod__ = __rmod__ = __pos__ = __neg__ = __call__ = \
        __getitem__ = __lt__ = __le__ = __gt__ = __ge__ = __int__ = \
        __float__ = __complex__ = __pow__ = __rpow__ = \
        _fail_with_undefined_error


def get_path(resource_type, package_name, resource_name):
    return "{}.{}.{}".format(resource_type, package_name, resource_name)

def get_model_path(package_name, resource_name):
    return get_path('models', package_name, resource_name)

def get_macro_path(package_name, resource_name):
    return get_path('macros', package_name, resource_name)

def __ref(model):

    def ref(*args):
        model_path = None

        if len(args) == 1:
            model_path = get_model_path(model.get('package_name'), args[0])
        elif len(args) == 2:
            model_path = get_model_path(args[0], args[1])
        else:
            dbt.utils.compiler_error(
                model.get('name'),
                "ref() takes at most two arguments ({} given)".format(
                    len(args)))

        model['depends_on'].append(model_path)

    return ref


def __config(model, cfg):

    def config(*args, **kwargs):
        if len(args) == 1 and len(kwargs) == 0:
            opts = args[0]
        elif len(args) == 0 and len(kwargs) > 0:
            opts = kwargs
        else:
            dbt.utils.compiler_error(
                model.get('name'),
                "Invalid model config given inline in {}".format(model))

        cfg.update_in_model_config(opts)

    return config


def parse_model(model, model_path, root_project_config,
                package_project_config):
    parsed_model = copy.deepcopy(model)

    parsed_model.update({
        'depends_on': [],
    })

    parts = dbt.utils.split_path(model.get('path', ''))
    name, _ = os.path.splitext(parts[-1])
    fqn = ([package_project_config.get('name')] +
            parts[1:-1] +
            [model.get('name')])

    config = dbt.model.SourceConfig(
        root_project_config, package_project_config, fqn)

    context = {
        'ref': __ref(parsed_model),
        'config': __config(parsed_model, config),
    }

    env = jinja2.sandbox.SandboxedEnvironment(
        undefined=SilentUndefined)

    env.from_string(model.get('raw_sql')).render(context)

    parsed_model['unique_id'] = model_path
    parsed_model['config'] = config.config
    parsed_model['empty'] = (len(model.get('raw_sql').strip()) == 0)
    parsed_model['fqn'] = fqn

    return parsed_model


def parse_models(models, projects):
    to_return = {}

    for model in models:
        package_name = model.get('package_name', 'root')

        model_path = get_model_path(package_name, model.get('name'))

        # TODO if this is set, raise a compiler error
        to_return[model_path] = parse_model(model,
                                            model_path,
                                            projects.get('root'),
                                            projects.get(package_name))

    return to_return


def load_and_parse_files(package_name, root_dir, relative_dirs, extension,
                         resource_type):
    file_matches = dbt.clients.system.find_matching(
        root_dir,
        relative_dirs,
        extension)

    models = []

    for file_match in file_matches:
        file_contents = dbt.clients.system.load_file_contents(
            file_match.get('absolute_path'))

        # TODO: support more than just models
        models.append({
            'name': os.path.basename(file_match.get('absolute_path')),
            'root_path': root_dir,
            'path': file_match.get('relative_path'),
            'package_name': package_name,
            'raw_sql': file_contents
        })

    return parse_models(models)


def load_and_parse_models(package_name, root_dir, relative_dirs):
    return load_and_parse_files(package_name, root_dir, relative_dirs,
                                extension="[!.#~]*.sql",
                                resource_type='models')
