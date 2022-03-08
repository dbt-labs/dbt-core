## Tracking next Releases

#### Purpose
This folder exists for changes that are slated for the next minor/major release but no `<version>.latest` branch exists for it yet.  The contents of this folder should never be backported.

#### Expectation
When we are ready to cut the next release, all yaml files under `.changes/unreleased` should have been backported and exists on the `<version>.latest` branch.  When that is confirmed, the contents of the `/unreleased` directory should be deleted and the contents of the `next` directory should replace it.

```rm ~/Projects/dbt-core/.changes/unreleased/*.yaml```
```mv ~/Projects/dbt-core/.changes/unreleased/next/*.yaml ~/Projects/dbt-core/.changes/unreleased```
