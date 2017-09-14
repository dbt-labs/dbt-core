# dbt

dbt (data build tool) helps analysts write reliable, modular code using a workflow that closely mirrors software development.

A dbt project primarily consists of "models". These models are SQL `select` statements that filter, aggregate, and otherwise transform data to facilitate analytics. Analysts use dbt to [aggregate pageviews into sessions](https://github.com/fishtown-analytics/snowplow), calculate [ad spend ROI](https://github.com/fishtown-analytics/facebook-ads), or report on [email campaign performance](https://github.com/fishtown-analytics/mailchimp).

These models frequently build on top of one another. Fortunately, dbt makes it easy to [manage relationships](https://dbt.readme.io/reference#ref) between models, [test](https://dbt.readme.io/docs/testing) your assumptions, and [visualize](https://graph.sinterdata.com/) your projects.

Still reading? Check out the [docs](https://dbt.readme.io/docs/overview) for more information.

![dbt dag](/etc/dag.png?raw=true)

---
### Getting Started

- [What is dbt]?
- Read the [dbt viewpoint]
- [Installation]
- Join the [chat][slack-url] on Slack for live questions and support.

---
### The dbt ecosystem
- Visualize your dbt graph [here](https://graph.sinterdata.com/)
- Run your dbt projects on a schedule [here](http://sinterdata.com/)

---

[![Code Climate](https://codeclimate.com/github/fishtown-analytics/dbt/badges/gpa.svg)](https://codeclimate.com/github/fishtown-analytics/dbt) [![Slack](https://ac-slackin.herokuapp.com/badge.svg)](https://ac-slackin.herokuapp.com)

### Testing

| service | development | master |
| --- | --- | --- |
| CircleCI| [![CircleCI](https://circleci.com/gh/fishtown-analytics/dbt/tree/development.svg?style=svg)](https://circleci.com/gh/fishtown-analytics/dbt/tree/development) | [![CircleCI](https://circleci.com/gh/fishtown-analytics/dbt/tree/master.svg?style=svg)](https://circleci.com/gh/fishtown-analytics/dbt/tree/master) |
| AppVeyor | [![AppVeyor](https://ci.appveyor.com/api/projects/status/v01rwd3q91jnwp9m/branch/development?svg=true)](https://ci.appveyor.com/project/DrewBanin/dbt/branch/development) | [![AppVeyor](https://ci.appveyor.com/api/projects/status/v01rwd3q91jnwp9m/branch/master?svg=true)](https://ci.appveyor.com/project/DrewBanin/dbt/branch/master) |

[Coverage](https://circleci.com/api/v1/project/fishtown-analytics/dbt/latest/artifacts/0/$CIRCLE_ARTIFACTS/htmlcov/index.html?branch=development)

## Code of Conduct

Everyone interacting in the dbt project's codebases, issue trackers, chat rooms, and mailing lists is expected to follow the [PyPA Code of Conduct].



[PyPA Code of Conduct]: https://www.pypa.io/en/latest/code-of-conduct/
[slack-url]: http://ac-slackin.herokuapp.com/
[Installation]: https://dbt.readme.io/docs/installation
[What is dbt]: https://dbt.readme.io/docs/overview
[dbt viewpoint]: https://dbt.readme.io/docs/viewpoint
