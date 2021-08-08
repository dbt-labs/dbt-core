#!/usr/bin/env python
from dataclasses import dataclass
from argparse import ArgumentParser
from pathlib import Path
import json
from typing import Type, Dict, Any

from dbt.contracts.graph.manifest import WritableManifest
from dbt.contracts.results import (
    CatalogArtifact, RunResultsArtifact, FreshnessExecutionResultArtifact
)
from dbt.contracts.util import VersionedSchema
from dbt.adapters.internal_storage import local_filesystem as local_SA


@dataclass
class ArtifactInfo:
    path: str
    name: str
    json_schema: Dict[str, Any]

    @classmethod
    def from_artifact_cls(
        cls,
        artifact_cls: Type[VersionedSchema],
    ) -> 'ArtifactInfo':
        return cls(
            path=artifact_cls.dbt_schema_version.path,
            name=artifact_cls.dbt_schema_version.name,
            json_schema=artifact_cls.json_schema()
        )

    def write_schema(self, dest_dir: Path):
        local_SA.write(
            str(dest_dir / self.path),
            json.dumps(self.json_schema, indent=2)
        )


@dataclass
class Arguments:
    path: Path

    @classmethod
    def parse(cls) -> 'Arguments':
        parser = ArgumentParser(
            prog="Collect and write dbt arfifact schema"
        )
        parser.add_argument(
            '--path',
            type=Path,
            help='The dir to write artifact schema',
        )

        parsed = parser.parse_args()
        return cls(
            path=parsed.path
        )


def collect_artifact_schema(args: Arguments):
    artifacts = [
        FreshnessExecutionResultArtifact,
        RunResultsArtifact,
        CatalogArtifact,
        WritableManifest
    ]
    artifact_infos = []
    for artifact_cls in artifacts:
        artifact_infos.append(ArtifactInfo.from_artifact_cls(artifact_cls))

    if args and args.path is not None:
        for artifact_info in artifact_infos:
            dest_dir = args.path.resolve()
            artifact_info.write_schema(dest_dir)
    else:
        artifacts_dict = {
            artifact_info.name: artifact_info.json_schema
            for artifact_info in artifact_infos
        }
        print(json.dumps(artifacts_dict))


def main():
    parsed = Arguments.parse()
    collect_artifact_schema(parsed)


if __name__ == '__main__':
    main()
