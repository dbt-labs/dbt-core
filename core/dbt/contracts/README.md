# Contracts README


## Artifacts

### Generating JSON schemas
A helper script, `scripts/collect-artifact-schema.py` is available to generate json schemas corresponding to versioned artifacts (`ArtifactMixin`s).

This script is necessary to run when a new artifact schema version is created, or when changes are made to existing artifact versions, and writes json schema to `schema/dbt/<artifact>/v<version>.json`.

Schemas in `schema/dbt` power the rendering in https://schemas.getdbt.com/ via https://github.com/dbt-labs/schemas.getdbt.com/

#### Example Usage

Available arguments:
```sh
❯ scripts/collect-artifact-schema.py --help
usage: Collect and write dbt arfifact schema [-h] [--path PATH] [--artifact {manifest,sources,run-results,catalog}]

options:
  -h, --help            show this help message and exit
  --path PATH           The dir to write artifact schema
  --artifact {manifest,sources,run-results,catalog}
                        The name of the artifact to update
```

Generate latest version of schemas of all artifacts to `schema/dbt/<artifact>/v<version>.json`
```sh
> sripts/collect-artifact-schema.py --path schemas
```

Generate latest version of schemas of manifest to `schema/dbt/manifest/v<version>.json`
```sh
> sripts/collect-artifact-schema.py --path schemas --artifact manifest
```
