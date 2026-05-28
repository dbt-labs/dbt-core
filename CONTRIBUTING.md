# Contributing to the dbt Fusion Engine

The dbt Fusion engine is the result of dozens of engineers working for multiple years to build a next generation data development toolbox. Currently, only a subset of functionality is open-sourced, but by Coalesce 2025, all dbt components through Level 1 SQL Comprehension will be available in this repo for contributions & improvements.

In addition to code contributions, there are many ways to contribute to the ongoing development of the dbt Fusion engine, such as by participating in discussions and issues. We encourage you to first read our higher-level document: "[Expectations for Open Source Contributors](https://docs.getdbt.com/community/resources/oss-expectations)".

### Notes
* **Adapters** - All adapter jinja logic is maintained in this repository. Please open up an issue here.
* **Database Drivers** - dbt Fusion uses the next generation ADBC protocol to submit queries. 
  * **For Snowflake** - please see the [Go Arrow Snowflake Driver](https://github.com/apache/arrow-adbc/tree/main/go/adbc/driver/snowflake)
  * **For BigQuery** - please see the [Go Arrow BigQuery Driver](https://github.com/apache/arrow-adbc/tree/main/go/adbc/driver/bigquery)
  * **For Postgres** - please see the [C Arrow Postgres Driver](https://github.com/dbt-labs/arrow-adbc/tree/main/c/driver/postgresql)
  
* **Branches** - All pull requests from community contributors should target the main branch (default). If the change is needed as a patch for a minor version of dbt that has already been released (or is already a release candidate), a maintainer will backport the changes in your PR to the relevant "latest" release branch (2.0.latest, 2.1.latest, ...). If an issue fix applies to a release branch, that fix should be first committed to the development branch and then to the release branch (rarely release-branch fixes may not apply to main).
* **Releases** - While the dbt Fusion engine is in beta, new releases will be cut approximately daily with fixes and new features. Versions will be labelled `2.0.0-beta.1`, `2.0.0-beta.2`, etc. Before filing a bug, please ensure that you have the latest release installed by running `dbt system update`. 

### Setting up an environment

#### Tools
dbt Fusion is written in Rust. Please make sure that you have the [rust toolchain installed](https://www.rust-lang.org/tools/install), along with the preferred testing utility [Nextest](https://nexte.st/). 

1. [Install Rust](https://www.rust-lang.org/tools/install)
2. Install the testing framework used for all tests & testbench configuration [Nextest](https://nexte.st/docs/installation/pre-built-binaries/)
3. Clone the repository `git clone https://github.com/dbt-labs/dbt-fusion.git`
4. `cd dbt-fusion`
5. `cargo build` for a debug build. `cargo build --release` for a release build

*There are no virtual environments needed!*


## Making a Change to dbt Fusion
Code can be merged into the current development branch main by opening a pull request. If the proposal looks like it's on the right track, then a dbt-fusion maintainer will triage the PR and label it as ready_for_review. From this point, two code reviewers will be assigned with the aim of responding to any updates to the PR within about one week. They may suggest code revision for style or clarity, or request that you add unit or integration test(s). These are good things! We believe that, with a little bit of help, anyone can contribute high-quality code. Once merged, your contribution will be available for the next release of dbt-fusion.

Automated tests run via GitHub Actions. If you're a first-time contributor, all tests (including code checks and unit tests) will require a maintainer to approve. Changes in the dbt-fusion repository trigger integration tests against Postgres. dbt Labs also provides CI environments in which to test changes to other adapters, triggered by PRs in those adapters' repositories, as well as periodic maintenance checks of each adapter in concert with the latest dbt-fusion code changes.

Once all tests are passing and your PR has been approved, a dbt-fusion maintainer will merge your changes into the active development branch. And that's it! Happy developing 🎉

## Adding a CHANGELOG Entry

We use [changie](https://changie.dev) to generate `CHANGELOG` entries. **Note:** Do not edit the `CHANGELOG.md` directly. Your modifications will be lost.

Follow the steps to [install `changie`](https://changie.dev/guide/installation/) for your system.

Once changie is installed and your PR is created for a new feature, simply run the following command and changie will walk you through the process of creating a changelog entry:

```shell
changie new
```

Commit the file that's created and your changelog entry is complete!

You don't need to worry about which `dbt-fusion` version your change will go into. Just create the changelog entry with `changie`, and open your PR against the `main` branch. All merged changes will be included in the next release of `dbt-fusion`.  If a changelog is not required, a maintainer can add the label `Skip Changelog` to bypass this requirement.