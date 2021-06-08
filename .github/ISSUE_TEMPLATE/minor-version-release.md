---
name: Minor version release
about: Creates a tracking checklist of items for a minor version release
title: "[Tracking] v#.##.# release "
labels: ''
assignees: ''

---

### Engineering TODOs:
- [ ] dbt-release workflow 
- [ ] Create new protected `x.latest` branch 
- [ ] Create a platform issue to update dbt Cloud
- [ ] Generate schema updates
- [ ] Bump plugin versions (dbt-spark + dbt-presto), add compatibility as needed
   - [ ]  Spark 
   - [ ]  Presto
- [ ] Create a platform issue to update dbt-spark versions to dbt Cloud 

### Product TODOs:
- [ ] Finalize migration guide (next.docs.getdbt.com)
- [ ] Release new version of dbt-utils with new dbt version compatibility. If there are breaking changes requiring a minor version, plan upgrades of other packages that depend on dbt-utils.
- [ ] Publish discourse
- [ ] Announce in dbt Slack
