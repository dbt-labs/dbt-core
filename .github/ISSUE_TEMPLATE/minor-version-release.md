---
name: Minor version release
about: Creates a tracking checklist of items for a minor version release
title: "[Tracking] v#.##.# release "
labels: ""
assignees: ""
---

### Release Core

- [ ] [Engineering] dbt-release workflow
- [ ] [Engineering] Create new protected `x.latest` branch
- [ ] [Product] Finalize migration guide (next.docs.getdbt.com)

### Release Cloud

- [ ] [Engineering] Create a platform issue to update dbt Cloud and verify it is completed
- [ ] [Engineering] Determine if schemas have changed. If so, generate new schemas and push to schemas.getdbt.com

### Announce

- [ ] [Product] Publish discourse
- [ ] [Product] Announce in dbt Slack

### Post-release

- [ ] [Engineering] [Bump plugin versions](https://www.notion.so/fishtownanalytics/Releasing-b97c5ea9a02949e79e81db3566bbc8ef#59571f5bc1a040d9a8fd096e23d2c7db) (dbt-spark + dbt-presto), add compatibility as needed
  - [ ] Spark
  - [ ] Presto
- [ ] [Engineering] Create a platform issue to update dbt-spark versions to dbt Cloud
- [ ] [Product] Release new version of dbt-utils with new dbt version compatibility. If there are breaking changes requiring a minor version, plan upgrades of other packages that depend on dbt-utils.
- [ ] [Engineering] If this isn't a final release, create an epic for the next release
