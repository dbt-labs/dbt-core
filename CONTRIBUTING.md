# Contributing to dbt Core

There are many ways to contribute to the ongoing development of dbt Core, including code, discussions, and issues. We encourage you to first read our higher-level document: "[Expectations for Open Source Contributors](https://docs.getdbt.com/community/resources/oss-expectations)".

### Notes
* **Adapters** - All adapter jinja logic is maintained in this repository. Please open up an issue here.
* **Database Drivers** - dbt Core uses the next generation ADBC protocol to submit queries. 
  * **For Snowflake** - please see the [Go Arrow Snowflake Driver](https://github.com/apache/arrow-adbc/tree/main/go/adbc/driver/snowflake)
  * **For BigQuery** - please see the [Go Arrow BigQuery Driver](https://github.com/apache/arrow-adbc/tree/main/go/adbc/driver/bigquery)
  * **For Postgres** - please see the [C Arrow Postgres Driver](https://github.com/dbt-labs/arrow-adbc/tree/main/c/driver/postgresql)
  
* **Branches** - All pull requests from community contributors should target the main branch (default). If the change is needed as a patch for a minor version of dbt that has already been released (or is already a release candidate), a maintainer will backport the changes in your PR to the release branch.
* **Releases** - While dbt Core v2.0 is in alpha, new releases will include fixes and new features. Versions will be labelled `2.0.0-alpha.1`, `2.0.0-alpha.2`, etc. Before filing a bug, please ensure that you have the latest release installed. 

### Setting up an environment

#### Tools
dbt Core is written in Rust. Please make sure that you have the [rust toolchain installed](https://www.rust-lang.org/tools/install), along with the preferred testing utility [Nextest](https://nexte.st/). 

1. [Install Rust](https://www.rust-lang.org/tools/install)
2. Install the testing framework used for all tests & testbench configuration [Nextest](https://nexte.st/docs/installation/pre-built-binaries/)
3. Clone the repository `git clone https://github.com/dbt-labs/dbt-core.git`
4. `cd dbt-core`
5. `cargo build` for a debug build. `cargo build --release` for a release build

*There are no virtual environments needed!*


## Making a Change to dbt Core
Contribute by opening a pull request against the current development branch, `main`. A dbt Core maintainer will triage the PR and, once it's on the right track, assign a reviewer. They may suggest code revisions for style or clarity, or request that you add test(s).

Once your PR has been approved, a maintainer will take it from there and shepherd your changes into dbt Core. And that's it! Happy developing 🎉

## Adding a CHANGELOG Entry

We use [changie](https://changie.dev) to generate `CHANGELOG` entries. **Note:** Do not edit the `CHANGELOG.md` directly. Your modifications will be lost.

Follow the steps to [install `changie`](https://changie.dev/guide/installation/) for your system.

Once changie is installed and your PR is created for a new feature, simply run the following command and changie will walk you through the process of creating a changelog entry:

```shell
changie new
```

Commit the file that's created and your changelog entry is complete!

You don't need to worry about which `dbt-core` version your change will go into. Just create the changelog entry with `changie`, and open your PR against the `main` branch. All merged changes will be included in the next release of `dbt-core`.  If a changelog is not required, a maintainer can add the label `Skip Changelog` to bypass this requirement.
