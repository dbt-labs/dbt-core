from dbt.adapters.base import Column
from dataclasses import dataclass
from dbt.adapters.postgres.relation import PostgresRelation


@dataclass(frozen=True, eq=False, repr=False)
class RedshiftRelation(PostgresRelation):
    # Override the method in the Postgres Relation
    # because Redshift allows longer names
    def relation_max_name_length(self):
        return 127


class RedshiftColumn(Column):
    pass  # redshift does not inherit from postgres here
