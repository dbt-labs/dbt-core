<p align="center">
  <img src="https://raw.githubusercontent.com/dbt-labs/dbt-core/fa1ea14ddfb1d5ae319d5141844910dd53ab2834/etc/dbt-core.svg" alt="dbt logo" width="750"/>
</p>

> [!WARNING]
> **dbt Core v1 development has moved to the [`1.latest`](https://github.com/dbt-labs/dbt-core/tree/1.latest) branch.**
> The `main` branch now hosts dbt Core v2.0 (alpha) — a ground-up rewrite in Rust that is the foundation of the Fusion engine. If you're looking for the Python implementation of dbt Core, switch to [`1.latest`](https://github.com/dbt-labs/dbt-core/tree/1.latest).

**[dbt](https://www.getdbt.com/)** enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.

![architecture](https://raw.githubusercontent.com/dbt-labs/dbt-core/202cb7e51e218c7b29eb3b11ad058bd56b7739de/etc/dbt-transform.png)

## About dbt Core v2.0

> 🚧 dbt Core v2.0 is in alpha. Behavior, APIs, and on-disk formats may change before the stable release.

dbt Core v2.0 is engineered for performance at scale — parsing, compiling, and running projects in a fraction of the time compared to v1. It's released under the Apache 2.0 license and is the foundation of the [Fusion engine](https://docs.getdbt.com/docs/fusion/about-fusion).

The big shifts from v1:

- **Faster** — parse and compile times are dramatically improved, especially on the largest dbt projects.
- **Stricter** — a tightly-defined language specification enforces correctness at parse time.
- **More scalable artifacts** — v2.0 produces Parquet artifacts that can be easily queried, joined, and analyzed to understand your dbt project. The artifacts encompass everything in the JSON artifacts (e.g. `manifest.json`), which continue to be produced for backwards compatibility.
- **Easier to install** — distributed as a single self-contained binary, with no Python runtime or dependency management required.
- **A completely revamped local documentation experience** — dbt docs is now powered by those new artifacts and capable of scaling to large projects.

### Supported operating systems and architectures

dbt Core v2.0 and its drivers are compiled per operating system and architecture.

Legend:
* 🟢 — Supported today
* 🟡 — Not yet supported

| Operating system | x86-64 | ARM |
|---|---|---|
| macOS | 🟢 | 🟢 |
| Linux | 🟢 | 🟢 |
| Windows | 🟢 | 🟡 |

## Understanding dbt

Analysts using dbt can transform their data by simply writing select statements, while dbt handles turning these statements into tables and views in a data warehouse.

These select statements, or "models", form a dbt project. Models frequently build on top of one another – dbt makes it easy to [manage relationships](https://docs.getdbt.com/docs/ref) between models, and [visualize these relationships](https://docs.getdbt.com/docs/documentation), as well as assure the quality of your transformations through [testing](https://docs.getdbt.com/docs/testing).

![dbt dag](https://raw.githubusercontent.com/dbt-labs/dbt-core/6c6649f9129d5d108aa3b0526f634cd8f3a9d1ed/etc/dbt-dag.png)

## Getting started

Start by choosing a distribution. dbt Core is the baseline distribution of dbt. Fusion extends dbt Core with additional SQL comprehension abilities. Both distributions are free to install and can run locally.

- **If you need an Apache 2.0 licensed tool** and the ability to review every line of code inside of it, [install dbt Core](https://docs.getdbt.com/docs/local/install-dbt#dbt-core).
- **If you need a free CLI you can use locally**, [install Fusion](https://docs.getdbt.com/docs/local/install-dbt#dbt-fusion-engine-recommended). It can do more than dbt Core out of the box and you can seamlessly enable other advanced features over time if you choose to. 

Regardless of the distribution you choose, each is part of a single framework with a single language specification, meaning your business logic is portable in both directions.

Explore the [dbt platform](https://docs.getdbt.com/docs/cloud/about-cloud/dbt-cloud-features) for an enhanced collaboration experience.
Read the [introduction](https://docs.getdbt.com/docs/introduction/) and [viewpoint](https://docs.getdbt.com/docs/about/viewpoint/)

## Join the dbt Community

- Be part of the conversation in the [dbt Community Slack](http://community.getdbt.com/)
- Read more on the [dbt Community Discourse](https://discourse.getdbt.com)

## Reporting bugs and contributing code

- Want to report a bug or request a feature? Let us know and open [an issue](https://github.com/dbt-labs/dbt-core/issues/new/choose)
- Want to help us build dbt? Check out the [Contributing Guide](https://github.com/dbt-labs/dbt-core/blob/HEAD/CONTRIBUTING.md)

## Code of Conduct

Everyone interacting in the dbt project's codebases, issue trackers, chat rooms, and mailing lists is expected to follow the [dbt Code of Conduct](https://docs.getdbt.com/community/resources/code-of-conduct).

## License

dbt Core is licensed under the [Apache License 2.0](LICENSE).

<!-- test change: no-op edit for a test PR -->

