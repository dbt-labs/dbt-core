import dataclasses


# TODO: remove from dbt.contracts.util:: Replaceable + references
class Replaceable:
    def replace(self, **kwargs):
        return dataclasses.replace(self, **kwargs)
