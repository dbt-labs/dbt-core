# dbt/artifacts

## Overview
This directory is meant to be a lightweight module that is independent (and upstream of) the rest of dbt-core internals.

It's primary responsibility is to define simple data classes that represent the versioned artifact schemas that dbt writes as JSON files throughout execution. 

Long term, this module may be released as a standalone package (e.g. dbt-artifacts) to support stable parsing dbt artifacts programmatically.

`dbt/artifacts` is organized into artifact 'schemas' and 'resources'. Schemas represent the final serialized artifact object, while resources represent sub-components of the larger artifact schemas.

### dbt/artifacts/schemas


Each major version of a schema under `dbt/artifacts/schema` is defined in its corresponding `dbt/artifacts/schema/<artifact-name>/v<version>` directory. Before `dbt/artifacts` artifact schemas were always modified in-place, which is why artifacts are missing class definitions for historical versions.

Currently, there are four artifact schemas defined in `dbt/artifact/schemas`:

| Artifact name | File             | Class                            | Latest definition                 |
|---------------|------------------|----------------------------------|-----------------------------------|
| manifest      | manifest.json    | WritableManifest                 | dbt/artifacts/schema/manifest/v11 |
| catalog       | catalog.json     | CatalogArtifact                  | dbt/artifacts/schema/catalog/v1   |
| run           | run_results.json | RunResultsArtifact               | dbt/artifacts/schema/run/v5       |
| freshness     | sources.json     | FreshnessExecutionResultArtifact | dbt/artifacts/schema/freshness/v3 |


### dbt/artifacts/resources

All existing resources are defined under `dbt/artifacts/resources/v1`.

## Making changes to dbt/artifacts

### Non-breaking changes

Freely make incremental, non-breaking changes in-place to the latest major version of any artifact in mantle (via minor or patch bumps). The only changes that are fully forward and backward compatible are: 
* Adding a new field with a default
* Deleting an __optional__ field

### Breaking changes
A breaking change is anything that: 
* Deletes a required field
* Changes the name or type of an existing field
* Removes default from a field

These should generally be avoided, and bundled together to aim for as minimal disruption across the integration ecosystem as possible. 

However, when it comes time to make one (or more) of these, a new versioned artifact should be created as follows: 
 1. Create a new version directory and file that defines the new artifact schema under `dbt/artifacts/schemas/<artifact>/v<next-artifact-version>/<artifact>.py`
 2. If any resources are having breaking changes introduced, create a new resource class that defines the new resource schema under `dbt/artifacts/resources/v<next-resource-version>/<resource>.py`
 3. Implement upgrade paths on the new versioned artifact class so it can be constructed given a dictionary representation of any previous version of the same artifact
     * TODO: update once the design is finalized
4. Implement downgrade paths on all previous versions of the artifact class so they can still be constructed given a dictionary representation of the new artifact schema
    * TODO: update once the design is finalized
5. Update the 'latest' aliases to point to the new version of the artifact and/or resource:
    * Artifact: `dbt/artifacts/schemas/<artifact>/__init__.py `
    * Resource: `dbt/artifacts/resources/__init__.py `

    Downstream consumers (e.g. dbt-core) importing from the latest alias are susceptible to breaking changes. Ideally, any incompatibilities should be caught my static type checking in those systems. However, it is always possible for consumers to pin imports to previous versions via `dbt.artifacts.schemas.<artifact>.v<prev-version>`
