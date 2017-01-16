# Package management

Every dbt project is a package that can be imported as a dependency within other projects. Importing external packages into a project is done in the `repositories` section of `dbt_project.yml` as such:

```YAML
# add dependencies. these will get pulled during the `dbt deps` process.

repositories:
  - "https://github.com/fishtown-analytics/snowplow.git"
```

Once a package is added as a dependency, updated source code for that dependency can be downloaded from its git repository with `dbt deps`. Package code will be available in the same dependency graph as the models within the current project. This allows your models to use `ref()` on top of models that are built and maintained by others in your organization.

## Open source dbt packages

Our goal is to make dbt packages built on top of common datasets able to be shared in an open source workflow, similar to how Python, R, and Docker allow you to easily download, import, and upgrade packages. While this is already possible, there are several key features that prevent this workflow from being very usable today, and as such we don't necessarily recommend it.

We want nothing more than to see a vibrant community of open source analytic models develop, and will be pushing hard in that direction once we're able to make the improvements to dbt that facilitate this workflow.
