from dataclasses import dataclass
from typing import Optional

from dbt.adapters.base.relation import (
    BaseRelation, ComponentName
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

    def information_schema(self: Self, identifier=None) -> Self:
        # BigQuery (usually) addresses information schemas at the dataset
        # level. This method overrides the BaseRelation method to return an
        # Information Schema relation as project.dataset.information_schem

        include_policy = self.include_policy.replace(
            database=self.database is not None,
            schema=self.schema is not None,
            identifier=True
        )

        # Quote everything on BigQuery -- identifiers are case-sensitive,
        # even when quoted.
        quote_policy = self.quote_policy.replace(
            database=True,
            schema=True,
            identifier=True,
        )

        path = self.path.replace(
            schema=self.schema,
            identifier='INFORMATION_SCHEMA'
        )

        return self.replace(
            quote_policy=quote_policy,
            include_policy=include_policy,
            path=path,
        )
