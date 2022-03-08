## 1.1.0 changed release TBD

#### Purpose
This folder exists for changes that are slated for the next minor/major release.  The contents of this folder should never be backported.

#### Expectation
When we are ready to cut the 1.1.0 release, all yaml files under `.changes/unreleased` should have been backported and exists on the `1.0.latest` branch..  When that is confirmed, the contents of that folder should be deleted and the contents of this folder should replace it.

```rm ~/Projects/dbt-core/.changes/unreleased/*.yaml```
```mv ~/Projects/dbt-core/.changes/1.1.unreleased/*.yaml ~/Projects/dbt-core/.changes/unreleased```

When the time comes to cut a new major/minor release, a new directory named for that release will need to be created.
