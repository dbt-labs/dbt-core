import dbt.exceptions

import dbt.context.common


execute = False


def ref(db_wrapper, model, config, manifest):

    def ref(*args):
        if len(args) == 1 or len(args) == 2:
            model.refs.append(list(args))

        else:
            dbt.exceptions.ref_invalid_args(model, args)

        return db_wrapper.adapter.Relation.create_from_node(config, model)

    return ref


def docs(unparsed, docrefs, column_name=None):

    def do_docs(*args):
        if len(args) != 1 and len(args) != 2:
            dbt.exceptions.doc_invalid_args(unparsed, args)
        doc_package_name = ''
        doc_name = args[0]
        if len(args) == 2:
            doc_package_name = args[1]

        docref = {
            'documentation_package': doc_package_name,
            'documentation_name': doc_name,
        }
        if column_name is not None:
            docref['column_name'] = column_name

        docrefs.append(docref)

        # IDK
        return True

    return do_docs


class Config:
    def __init__(self, model, source_config):
        self.model = model
        self.source_config = source_config

    def _transform_config(self, config):
        for oldkey in ('pre_hook', 'post_hook'):
            if oldkey in config:
                newkey = oldkey.replace('_', '-')
                if newkey in config:
                    dbt.exceptions.raise_compiler_error(
                        'Invalid config, has conflicting keys "{}" and "{}"'
                        .format(oldkey, newkey),
                        self.model
                    )
                config[newkey] = config.pop(oldkey)
        return config

    def __call__(self, *args, **kwargs):
        if len(args) == 1 and len(kwargs) == 0:
            opts = args[0]
        elif len(args) == 0 and len(kwargs) > 0:
            opts = kwargs
        else:
            dbt.exceptions.raise_compiler_error(
                "Invalid inline model config",
                self.model)

        opts = self._transform_config(opts)

        self.source_config.update_in_model_config(opts)
        return ''

    def set(self, name, value):
        return self.__call__({name: value})

    def require(self, name, validator=None):
        return ''

    def get(self, name, validator=None, default=None):
        return ''


def generate(model, runtime_config, manifest, source_config):
    return dbt.context.common.generate(
        model, runtime_config, manifest, source_config, dbt.context.parser)
