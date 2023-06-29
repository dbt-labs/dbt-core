#!/usr/bin/env python
from dataclasses import dataclass
from argparse import ArgumentParser
from pathlib import Path
import json
from typing import Type, Dict, Any

from dbt.contracts.graph.manifest import WritableManifest
from dbt.contracts.results import (
    CatalogArtifact,
    RunResultsArtifact,
    FreshnessExecutionResultArtifact,
)
from dbt.contracts.util import VersionedSchema
from dbt.clients.system import write_file


@dataclass
class ArtifactInfo:
    path: str
    name: str
    json_schema: Dict[str, Any]

    @classmethod
    def from_artifact_cls(
        cls,
        artifact_cls: Type[VersionedSchema],
    ) -> "ArtifactInfo":
        return cls(
            path=artifact_cls.dbt_schema_version.path,
            name=artifact_cls.dbt_schema_version.name,
            json_schema=artifact_cls.json_schema(),
        )

    def write_schema(self, dest_dir: Path):
        write_file(str(dest_dir / self.path), json.dumps(self.json_schema, indent=2))


@dataclass
class Arguments:
    artifact: str
    path: Path

    @classmethod
    def parse(cls) -> "Arguments":
        parser = ArgumentParser(prog="Collect and write dbt arfifact schema")
        parser.add_argument(
            "--path",
            type=Path,
            help="The dir to write artifact schema",
        )

        parser.add_argument(
            "--artifact",
            type=str,
            choices=["manifest", "sources", "run-results", "catalog"],
            help="The name of the artifact to update",
        )

        parsed = parser.parse_args()
        return cls(artifact=parsed.artifact, path=parsed.path)


def collect_artifact_schema(args: Arguments):
    artifacts = [
        FreshnessExecutionResultArtifact,
        RunResultsArtifact,
        CatalogArtifact,
        # WritableManifest introduces new definitions in hologram which are likely
        # getting persisted across invocations of json_schema and making their
        # way to other written artifacts - so write it last as a short-term fix.
        # https://github.com/dbt-labs/dbt-core/issues/7604
        WritableManifest,
    ]
    filtered_artifacts = filter(
        lambda a: a.dbt_schema_version.name == args.artifact or args.artifact is None, artifacts
    )
    artifact_infos = []
    for artifact_cls in filtered_artifacts:
        artifact_infos.append(ArtifactInfo.from_artifact_cls(artifact_cls))

    if args and args.path is not None:
        for artifact_info in artifact_infos:
            dest_dir = args.path.resolve()
            artifact_info.write_schema(dest_dir)
    else:
        artifacts_dict = {
            artifact_info.name: artifact_info.json_schema for artifact_info in artifact_infos
        }
        print(json.dumps(artifacts_dict))


def main():
    parsed = Arguments.parse()
    collect_artifact_schema(parsed)


if __name__ == "__main__":
    main()
