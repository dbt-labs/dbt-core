# dbt: New Engine, Same Language (May 2025)

Hello! Today was one of our biggest launches in dbt Labs history. We announced a bunch of new experiences for the dbt platform, which you can read about in [the roundup](https://www.getdbt.com/blog/dbt-launch-showcase-2025-recap).

And we finally got to announce one big thing that we’ve been working on with [the team that joined us from SDF](https://www.getdbt.com/blog/dbt-labs-acquires-sdf-labs) — the **dbt Fusion engine**, written in Rust and optimized for speed and scale.

Since [last December](https://github.com/dbt-labs/dbt-core/blob/main/docs/roadmap/2024-12-play-on.md), the Core team at dbt Labs has been busy developing new features for the [dbt Core v1.10 release](https://docs.getdbt.com/docs/dbt-versions/core-upgrade/upgrading-to-v1.10) (`rc1` available now!) …

| **Version** | **When** | **Namesake** | **Stuff** |
| --- | --- | --- | --- |
| v1.10 | May (beta) | TBD | `--sample` mode. Catalogs. Macro argument validation. Calculate source freshness via a custom `loaded_at_query`. Deprecation warnings using new and improved jsonschemas. |

… and **we’ve been building a [whole new dbt engine](https://github.com/dbt-labs/dbt-fusion) from the ground up**.

First thing’s first:

- We’re committed to maintaining dbt Core **indefinitely**, under the Apache 2.0 license.
- Fusion will be available under ELv2. That means you can use Fusion internally in your own business, for free and without restriction, forever. A bunch of its components (dbt-jinja, adapters, grammars, specs) will also be available under Apache 2.0.
- You can read all the fine print in the [Licensing FAQs](https://www.getdbt.com/licenses-faq).

Let’s talk about what’s changing, and what’s staying the same.

## What’s the difference between dbt Core and the new dbt Fusion engine?

The new dbt Fusion **engine** is brand-new code. It’s built for speed and SQL comprehension, meaning it already has some underlying capabilities that dbt Core doesn’t.

The **language** is the same: it’s dbt. It’s the language you’ve learned to do your job, and learned to love because it’s how you get your job done.

```
    .--------------.
   / dbt language   \
  |    .--------.    |
  |   /          \   |
  |  | dbt engine |  |  
  |   \          /   |
  |    '--------'    |
   \                /
    '--------------'

caption: the dbt framework
```

We’ve written about this difference between “language” and the “engine” in the FAQs (linked above), and we’re going to give some more specific examples below. The really important message is this one:

Together, the **language** and the **engine** create **the dbt framework.**

### What is the dbt language?

The “language” of dbt includes anything you can write in your dbt project. Some of us like to call this the “authoring layer” — what we mean is, *the code you can put in your project.* Every data test configuration, every unit test property, the `+` before configs in `dbt_project.yml`, everything you can put between `{% macro %}` and `{% endmacro %}` ([just one, though](https://github.com/dbt-labs/dbt-core/issues/11393)). The language also includes important abstractions, like the idea that one `.sql` file in the `models/` directory == one dbt model == one data warehouse object ([or none](https://docs.getdbt.com/docs/build/materializations#ephemeral)).

This language is really important. You’ve invested years into learning it, and you’ve used it to define your organization’s critical business logic. That’s why we’re committed to supporting, improving, and expanding the language across both dbt Core and Fusion.

Recently, that has included important work to *tighten up* the language with stricter validation of your project code, as discussed in [dbt-core#11493](https://github.com/dbt-labs/dbt-core/discussions/11493). We want dbt to give more-helpful feedback when you misspell a config or write bad code in your project — a longtime paper-cut we are thrilled to at last be solving (see: [#2606,](https://github.com/dbt-labs/dbt-core/issues/2606) [#4280, #5605](https://github.com/dbt-labs/dbt-core/issues/5605), [#8942](https://github.com/dbt-labs/dbt-core/issues/8942)). dbt Core v1.10 is firing deprecation warnings, and Fusion will raise errors — and both engines are using the *same strongly-typed schemas* do it. Those schemas are live in the [dbt-jsonschema](https://github.com/dbt-labs/dbt-jsonschema/tree/main/schemas/latest_fusion) repo (under an Apache 2.0 license), and they’ll keep getting better as we work through prerelease feedback for dbt Core v1.10 and Fusion.

We also want to keep adding language features across both Core and Fusion. Because Fusion is newer technology and has additional capabilities, such as built-in SQL comprehension, some language features will be better or exclusively available on Fusion. Think: an _enhancement_ to `--sample` and `--empty` modes where the engine intelligently add filters and limits, [without the associated issues of subqueries](https://github.com/dbt-labs/dbt-adapters/issues/199).

### What is the dbt engine?

The “engine” of dbt is the foundational technology for turning **the code in your dbt project into data platform reality*.* The engine:

- takes your project code, validates it (against the dbt language spec), and turns it into a DAG
- connects to remote data warehouse using **adapters**
- executes that DAG in order — creating, updating, or queries data in the warehouse — based on the commands/flags/arguments you pass in
- produces logs and metadata from that DAG execution

These things may differ across the dbt Core and Fusion engines. To ease the upgrade path, Fusion supports most of the same CLI commands/flags/arguments as dbt Core, and Fusion adapters support the same authentication methods as Core adapters. But we’re not planning for exact conformance on logs, metadata, and every single runtime behavior. 

Two examples from [the upgrade guide](https://docs.getdbt.com/docs/dbt-versions/core-upgrade/upgrading-to-fusion):

- Fusion can run unit tests first before building *any* models, because it can infer column schemas from SQL understanding.
- There’s no `--partial-parse` flag for Fusion, because its project parsing is *just that much faster. (And it will manage the cache itself)*

If there are things you think are great about the dbt Core engine, and you want Fusion to meet the same need — let us know! We’ve got some tips at the end about how to engage with the next few months of Fusion development.

## What’s next for dbt-the-language?

We’ve been building the new dbt Fusion engine since January, and we’ve still got plenty more to go. Our biggest focus area for the next few months is stabilizing and enhancing Fusion to support more of dbt's existing features, and more existing dbt projects. ([Joel and Jeremy wrote about Fusion’s path to GA](https://docs.getdbt.com/blog/dbt-fusion-engine-path-to-ga).)

At the same time, we know there’s appetite for new capabilities in the dbt language — including some ideas we’ve been talking about for a long time.

Importantly, our goal isn’t just parity with dbt as it exists today. We intend to keep expanding the language, across both the Core and Fusion engines. In many cases, we will be able to deliver an even-better experience in Fusion, thanks to its speed and SQL awareness.

The exact specs here are TBD, but here are some features we’ve been thinking about adding:

- **Out-of-the-box support for UDFs**
- **Sources from external catalogs**
- **Model freshness checks**
- **…along with Bugs, Polish Work, and Paper Cuts**

To be clear, we’re not committing to building precisely these things on any specific timeline — our top priority is parity for the new engine, to support existing users and customers in upgrading to Fusion — but we’re including these ideas as an illustration of what the same-framework, multi-engine future could look like. We’d love your thoughts on these ideas — what would be most interesting/useful to you?

### **Out-of-the-box support for UDFs**

The idea to manage user-defined functions (UDFs) with dbt is almost [as old as dbt](https://github.com/dbt-labs/dbt-core/issues/136) — and it’s one that has [come up](https://github.com/dbt-labs/dbt-core/discussions/5099) [every few](https://github.com/dbt-labs/dbt-core/discussions/5741) [years since](https://github.com/dbt-labs/dbt-core/discussions/10395).

UDFs enable users to define and register custom functions within the warehouse. Like dbt macros, they enable DRY reuse of code; unlike macros, UDFs can be defined in languages other than SQL (Python, Java, Scala, …) and they can be used by queries outside of dbt.

There are two direct benefits to dbt-managed UDFs:

1. dbt will manage (create, update, rename) UDFs as part of DAG execution. If you want to update a UDF *and* a model that calls it, you can test the changes together in a development environment; propose those changes in a single pull request that goes through CI; and deploy them together into your production environment. dbt will ensure that the UDF is created before attempting to build the model that references it. And we could even imagine supporting *unit tests* on UDFs — which are functions, after all!
2. Fusion’s SQL comprehension is dialect-aware, but it is not yet *UDF-aware.* Today, if you call UDFs within a dbt model, you need to turn off Fusion’s static analysis for that model’s SQL. By supporting UDFs in the dbt framework, Fusion can also support them in its static analysis. We’re taking inspiration from Fusion’s antecedent, SDF, which supported UDFs for exactly this reason: https://docs.sdf.com/guide/advanced/udf

Imagine if the dbt framework knew about user-defined functions. The Core engine could manage the creation of UDFs as data warehouse objects. The Fusion engine could take it one step further, by *also* validating your UDFs’ SQL and statically analyzing the SQL of dbt models that call those UDFs. 

### **Sources from external catalogs**

Back in December, we discussed ([dbt-core#11171](https://github.com/dbt-labs/dbt-core/discussions/11171)) our plans for integrating catalogs: Glue, Iceberg, Snowflake Managed, Unity, …

Our goal was to centralize the configuration for materializing Iceberg tables, try to abstract over the minute differences between catalog providers (where possible), and teach dbt about a new top-level construct (`catalogs`).

You can check out [the docs on Iceberg catalogs](https://docs.getdbt.com/docs/mesh/iceberg/about-catalogs), new in dbt Core v1.10.

The first supported use case for external catalogs is around materializing dbt models *as Iceberg tables* by *writing them to catalogs* — but we know that another popular use case is reading from source tables that have been ingested to Iceberg catalogs, a.k.a. bringing the functionality of [read-external-iceberg from the dbt-labs-experimental-features](https://github.com/dbt-labs/dbt-labs-experimental-features/tree/main/read-external-iceberg) repository [into the dbt framework](https://github.com/dbt-labs/dbt-core/discussions/11265):

```sql
select * from {{ source('my_catalog', 'my_iceberg_table') }}
```

You can already hack this with the [experimental code](https://www.notion.so/Core-Roadmap-Post-5-28-1f8bb38ebda780ce8dd4ec19a7411fc7?pvs=21) today — but we see an opportunity to build it into dbt out-of-the-box, and to standardize the configurations for everyone interacting with Iceberg tables, for both use cases (sources and models) in their dbt projects.

### **Model freshness checks**

dbt Core has supported `source freshness` checks since the [v0.13 release in 2019](https://github.com/dbt-labs/dbt-core/releases/tag/v0.13.0) (!). The idea, then and now, is: for tables that are updated by processes outside of dbt, the least dbt can do is ask, “How fresh is in the data in this table?”

By contrast, it should be simple for dbt to know the freshness in the models *that dbt is building*… right?

In practice, this can be trickier than you’d think! Multiple dbt invocations, split across different jobs/tasks/DAGs/schedules/orchestrators/projects/…, can result in confusion about when a model actually last built. Users have closed the gap with generic tests in popular packages, such as [dbt_utils.recency](https://github.com/dbt-labs/dbt-utils?tab=readme-ov-file#recency-source) and [dbt_expectations.expect_row_values_to_have_recent_data](https://github.com/metaplane/dbt-expectations?tab=readme-ov-file#expect_row_values_to_have_recent_data) — but the idea that [*dbt should know about model freshness*](https://github.com/dbt-labs/dbt-core/discussions/5103) is one we’ve tossed around a few times.

This is a case where we think we might be able to introduce a new language concept that powers a capability in both engines, and unlock something additional in Fusion. Imagine:

- The dbt language introduces `freshness` as an optional config for models.
- dbt Core, which is stateless, could execute `freshness` checks on models — using a configured column, or metadata from the data warehouse. Fusion could support this check, too.
- And: When the dbt Fusion engine is connected to the dbt platform, [it can run with *state awareness*](https://docs.getdbt.com/docs/deploy/state-aware-about). Fusion checks the freshness of upstream data inputs, and users can configure `freshness.build_after` — to reduce overbuilding models when there’s no new data, or when users want to save costs by placing an upper limit on build frequency.

```yaml
models:
  - name: stg_orders
    config:
      freshness:
	      # Fusion-powered state-aware orchestration: build this model after 4 hours, as long as it has new data
        build_after: {count: 4, period: hour}
        
        # Future: check that model has successfully updated within expected SLA, warn/error owner otherwise
        warn_after: {count: 24, period: hour}
        error_after: {count: 48, period: hour}
```

### **Bugs, Polish Work, and Paper Cuts**

And then, there’s everything else.

While our team has been focused on the v1.10 release of dbt Core, and building the new dbt Fusion engine, we’ve had less capacity to triage every new issue and externally contributed PR. Thank you for your patience over the past five months — and thank you to the community members who gave us the feedback that we need to ensure maintenance even while working on the exciting new stuff.

Over the coming months, alongside our work ensuring Fusion framework parity, we will be tracking our backlog of:

- externally-contributed PRs
- [polishing on microbatch](https://github.com/dbt-labs/dbt-core/issues/11292)
- polishing on jsonschemas (the long-tail of adapter-specific configs)
- `paper_cut`s

## How can you, a community member, contribute?

Mostly, in all the same ways as before: [https://docs.getdbt.com/community/resources/contributor-expectations](https://docs.getdbt.com/community/resources/contributor-expectations)

In the next few weeks, we’d really like your help — trying out the new engine, mettle-testing the new jsonschemas against your project code, and opening up bug issues when you run into problems — as we get Fusion (and the stricter dbt language spec) ready for prime-time. 

You can contribute your ideas by opening GitHub discussions or feature requests. While we’re focused on getting dbt Fusion to GA, we're going to keep the `dbt-fusion` repo focused on engine bugs and framework parity, so we’ll keep net-new feature requests in the `dbt-core` repo for the foreseeable future. Be on the lookout for GitHub discussions about new framework features, once we’re ready to start building.

You can contribute your code by opening pull requests against *any* of our repos — whether you’ve written Rust before, or you’re looking for an excuse to get started :)

## Let’s get to work

Maintaining a common framework across two codebases, written in entirely different languages, will be challenging. We’ve got some ideas for how to make this easier:

- Core and Fusion are already sharing a set of strongly-typed jsonschemas defining the spec of acceptable project code inputs
- Core adapters and Fusion adapters could share the same “packages” of macros and materializations
- *… and more to come …*

No lie — it’s going to be tricky, any way we slice it. We’re going to keep figuring it out over the coming months, and we ask for your patience while we do.

After all, [our commitment](https://www.youtube.com/watch?v=DC9sbZBYzpI) is to you, the community, not to any one codebase. We’re excited to welcome in some new codebases, and some new contributors, to this thing we’re all building together.

(╭☞ ͡° ͜ʖ ͡°)╭☞

**J**erco
**E**lias
**G**race
