from dataclasses import dataclass
from typing import Optional

from dbt.adapters.base.relation import (
    BaseRelation, ComponentName, InformationSchema
)
from dbt.utils import filter_null_values
from typing import TypeVar


Self = TypeVar('Self', bound='BigQueryRelation')


@dataclass(frozen=True, eq=False, repr=False)
class BigQueryRelation(BaseRelation):
    quote_character: str = '`'

    def matches(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        identifier: Optional[str] = None,
    ) -> bool:
        search = filter_null_values({
            ComponentName.Database: database,
            ComponentName.Schema: schema,
            ComponentName.Identifier: identifier
        })

        if not search:
            # nothing was passed in
            pass

        for k, v in search.items():
            if not self._is_exactish_match(k, v):
                return False

        return True

    @property
    def project(self):
        return self.database

    @property
    def dataset(self):
        return self.schema

    def information_schema(
        self, identifier: Optional[str] = None
    ) -> 'BigQueryInformationSchema':
        return BigQueryInformationSchema.from_relation(self, identifier)


@dataclass(frozen=True, eq=False, repr=False)
class BigQueryInformationSchema(InformationSchema):
    quote_character: str = '`'

    @classmethod
    def get_include_policy(cls, relation, information_schema_view):
        schema = True
        if information_schema_view in ('SCHEMATA', 'SCHEMATA_OPTIONS', None):
            schema = False

        identifier = True
        if information_schema_view == '__TABLES__':
            identifier = False

        return relation.include_policy.replace(
            schema=schema,
            identifier=identifier,
        )

    def replace(self, **kwargs):
        if 'information_schema_view' in kwargs:
            view = kwargs['information_schema_view']
            # we also need to update the include policy, unless the caller did
            # in which case it's their problem
            if 'include_policy' not in kwargs:
                kwargs['include_policy'] = self.get_include_policy(self, view)
        return super().replace(**kwargs)
