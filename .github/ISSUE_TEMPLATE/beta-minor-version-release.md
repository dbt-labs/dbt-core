---
name: Beta minor version release
about: Creates a tracking checklist of items for a Beta minor version release
title: "[Tracking] v#.##.#B# release "
labels: 'release'
assignees: ''

---

### Release Core
- [ ] [Engineering] Follow [dbt-release workflow](https://www.notion.so/dbtlabs/Releasing-b97c5ea9a02949e79e81db3566bbc8ef#03ff37da697d4d8ba63d24fae1bfa817) 
- [ ] [Engineering] Verify new release branch is created in the repo
- [ ] [Product] Finalize migration guide (next.docs.getdbt.com)

### Release Cloud
- [ ] [Engineering] Create a platform issue to update dbt Cloud and verify it is completed. [Example issue](https://github.com/dbt-labs/dbt-cloud/issues/3481)
- [ ] [Engineering] Determine if schemas have changed. If so, generate new schemas and push to schemas.getdbt.com

### Announce
- [ ] [Product] Announce in dbt Slack

### Post-release
- [ ] [Engineering] [Bump plugin versions](https://www.notion.so/dbtlabs/Releasing-b97c5ea9a02949e79e81db3566bbc8ef#f01854e8da3641179fbcbe505bdf515c) (dbt-spark + dbt-presto), add compatibility as needed
   - [ ]  [Spark](https://github.com/dbt-labs/dbt-spark) 
   - [ ]  [Presto](https://github.com/dbt-labs/dbt-presto)
- [ ] [Engineering] Create a platform issue to update dbt-spark versions to dbt Cloud. [Example issue](https://github.com/dbt-labs/dbt-cloud/issues/3481)
- [ ] [Engineering] Create an epic for the RC release
