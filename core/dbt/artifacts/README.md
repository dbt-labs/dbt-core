# dbt/artifacts

## Overview
This directory is meant to be a lightweight module that is independent (and upstream of) the rest of `dbt-core` internals.

Its primary responsibility is to define simple data classes that represent the versioned artifact schemas that dbt writes as JSON files throughout execution. 

Eventually, this module may be released as a standalone package (e.g. `dbt-artifacts`) to support stable programmatic parsing of dbt artifacts.

`dbt/artifacts` is organized into artifact 'schemas' and 'resources'. Schemas represent the final serialized artifact objects, while resources represent smaller components within those schemas.

### dbt/artifacts/schemas

Each major version of a schema under `dbt/artifacts/schema` is defined in its corresponding `dbt/artifacts/schema/<artifact-name>/v<version>` directory. Before `dbt/artifacts` artifact schemas were always modified in-place, which is why older artifacts are those missing class definitions.

Currently, there are four artifact schemas defined in `dbt/artifact/schemas`:

| Artifact name | File             | Class                            | Latest definition                 |
|---------------|------------------|----------------------------------|-----------------------------------|
| manifest      | manifest.json    | WritableManifest                 | dbt/artifacts/schema/manifest/v12 |
| catalog       | catalog.json     | CatalogArtifact                  | dbt/artifacts/schema/catalog/v1   |
| run           | run_results.json | RunResultsArtifact               | dbt/artifacts/schema/run/v5       |
| freshness     | sources.json     | FreshnessExecutionResultArtifact | dbt/artifacts/schema/freshness/v3 |


### dbt/artifacts/resources

All existing resources are defined under `dbt/artifacts/resources/v1`.

## Making changes to dbt/artifacts

### All changes

All changes to any fields will require a manual update to [dbt-jsonschema](https://github.com/dbt-labs/dbt-jsonschema) to ensure live checking continues to work.

### Non-breaking changes

Freely make incremental, non-breaking changes in-place to the latest major version of any artifact (minor or patch bumps). The only changes that are fully forward and backward compatible are:
* Adding a new field with a default
* Deleting a field with a default. This is compatible in terms of serialization and deserialization, but still may be lead to suprising behaviour:
    * For artifact consumers relying on the fields existence (e.g. `manifest["deleted_field"]` will stop working unless the access was implemented safely)
    * Old code (e.g. in dbt-core) that relies on the value of the deleted field may have surprising behaviour given only the default value will be set when instantiated from the new schema

These types of minor, non-breaking changes are tested by [tests/unit/artifacts/test_base_resource.py::TestMinorSchemaChange](https://github.com/dbt-labs/dbt-core/blob/main/tests/unit/artifacts/test_base_resource.py).


#### Updating [schemas.getdbt.com](https://schemas.getdbt.com)
Non-breaking changes to artifact schemas require an update to the corresponding jsonschemas published to [schemas.getdbt.com](https://schemas.getdbt.com), which are defined in https://github.com/dbt-labs/schemas.getdbt.com. To do so: 
Note this must be done AFTER the core pull request is merged, otherwise we may end up with unresolvable conflicts and schemas that are invalid prior to base pull request merge. You may create the schemas.getdbt.com pull request prior to merging the base pull request, but do not merge until afterward.
1. Create a PR in https://github.com/dbt-labs/schemas.getdbt.com which reflects the schema changes to the artifact. The schema can be updated in-place for non-breaking changes. Example PR: https://github.com/dbt-labs/schemas.getdbt.com/pull/39
2. Merge the https://github.com/dbt-labs/schemas.getdbt.com PR

Note: Although `jsonschema` validation using the schemas in [schemas.getdbt.com](https://schemas.getdbt.com) is not encouraged or formally supported, `jsonschema` validation should still continue to work once the schemas are updated because they are forward-compatible and can therefore be used to validate previous minor versions of the schema.

### Breaking changes
A breaking change is anything that:
* Deletes a required field
* Changes the name or type of an existing field
* Removes the default value of an existing field

These should be avoided however possible. When necessary, multiple breaking changes should be bundled together, to aim for minimal disruption across the ecosystem of tools that leverage dbt metadata. 

When it comes time to make breaking changes, a new versioned artifact should be created as follows: 
 1. Create a new version directory and file that defines the new artifact schema under `dbt/artifacts/schemas/<artifact>/v<next-artifact-version>/<artifact>.py`
 2. If any resources are having breaking changes introduced, create a new resource class that defines the new resource schema under `dbt/artifacts/resources/v<next-resource-version>/<resource>.py`
 3. Implement upgrade paths on the new versioned artifact class so it can be constructed given a dictionary representation of any previous version of the same artifact
     * TODO: link example once available
4. Implement downgrade paths on all previous versions of the artifact class so they can still be constructed given a dictionary representation of the new artifact schema
    * TODO: link example once available
5. Update the 'latest' aliases to point to the new version of the artifact and/or resource:
    * Artifact: `dbt/artifacts/schemas/<artifact>/__init__.py `
    * Resource: `dbt/artifacts/resources/__init__.py `

Downstream consumers (e.g. `dbt-core`) importing from the latest alias are susceptible to breaking changes. Ideally, any incompatibilities should be caught my static type checking in those systems. However, it is always possible for consumers to pin imports to previous versions via `dbt.artifacts.schemas.<artifact>.v<prev-version>`.
