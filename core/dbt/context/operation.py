import dbt.context.common
from dbt.context import runtime
from dbt.exceptions import raise_compiler_error


class RefResolver(runtime.RefResolver):
    def __call__(self, *args):
        # When you call ref(), this is what happens at operation runtime
        target_model, name = self.resolve(args)
        return self.create_relation(target_model, name)

    def create_ephemeral_relation(self, target_model, name):
        # In operations, we can't ref() ephemeral nodes, because ParsedMacros
        # do not support set_cte
        raise_compiler_error(
            'Operations can not ref() ephemeral nodes, but {} is ephemeral'
            .format(target_model.name),
            self.model
        )


class Provider(runtime.Provider):
    ref = RefResolver


def generate(model, runtime_config, manifest):
    return dbt.context.common.generate_execute_macro(
        model, runtime_config, manifest, Provider()
    )
