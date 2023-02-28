# dbt: Back to basics (February 2023)

We're back, and there's a lot to say—so much that if we're not mindful, we risk writing a(nother) novella. We're going to try for concision this time; it's a year of focus. Of course, a lot more has already been written, so we'll also link to those issues & discussions, where we encourage you to weigh in with thoughts.

Since last August, we've released two new versions of dbt Core:
- v1.3 unleashed Python models onto the world. This is very new functionality, and we are still gathering feedback on where to go next. Read & comment on [the big ideas](https://github.com/dbt-labs/dbt-core/discussions/categories/ideas?discussions_q=label%3Apython_models+category%3AIdeas).
- v1.4 reworked some internals, paid down some tech debt, and paved the way for better APIs going forward.

| Version | When | Namesake | Stuff |
| ------- | ------------- | -------------- | ----- |
| 1.3 ✅ | Oct 2022 | Edgar Allen Poe | Python models in dbt. More improvements to metrics. |
| 1.4 ✅ | Jan 2023 | Alain LeRoy Locke | Behind-the-scenes improvements to technical interfaces, especially structured logging. |

This year, we're returning to our fundamentals. This means fewer big surprises—fewer new answers to the question, "What is dbt?"—and more of the things that dbt needs—the dbt that you know, the dbt that you & your teams have come to rely on for getting your job done.

There are four big themes we want to tackle, reflected in the questions below. We will aim to provide compelling answers, sometimes with new functionality, sometimes with polish on top of existing capabilities. These aren't the only questions that interest us, but they are the ones we're prioritizing this year.

1. **Our APIs.** How can we enable the thousands of people who want to use dbt today, ? Can we enable community members to build more powerful extensions of dbt's framework, by exposing more & more of its functionality as a stable Python library? Can we provide an experience as delightful as dbt-core's CLI, via a reliable `dbt-server`, and RESTful APIs in dbt Cloud?
2. **Your models, as APIs.** Can dbt as a framework scale to complex deployments, across multiple teams, entering their third or fourth year of project maturity? Can it scale to some of the largest organizations who have adopted it as a standard pattern? What can we learn from the scaling challenges that software teams have encountered and surmounted over the last decade?
3. **Streaming.** How must dbt's essential building blocks—models, tests, sources, materializations—change (or not) to finally leverage data platforms' capabilities around streaming transformation?
4. **Semantic Layer.** How can we combine the existing power of dbt metrics, defined as an extension of your dbt DAG, with the depth of MetricFlow (!) as a framework for defining richer metrics and generating optimized queries?

In a sentence: **The same dbt, for more people.** More community members who might build plugins and extensions, without having to read `dbt-core` source code and hack together undocumented internal methods. More embedded analysts who can confidently contribute the right change to the right model in the right project, without having to first navigate through thousands of preexisting models with unclear ownership. More use cases that can be solved in "the dbt way," for batch as well as streaming. More downstream queriers who can benefit from asking questions on top of a dbt Semantic Layer.

We're sticking with one minor version release every three months. There won't be a version dedicated to _just_ API improvements, multi-project deployments, streaming, or semantic layer. Rather, we expect to make incremental progress as we go along. With that, here's our near-sighted lay of the land:

| Version | When          | Stuff          | Confidence |
| ------- | ------------- | -------------- | ---------- |
| 1.5 ⚒️ | April | An initial Python API for programmatic invocations, and a cleaner CLI to match. The beginning of multi-project deployments ("Models as APIs"), and of support for streaming (materialized views). | 95% |
| 1.6 🌀 | July | Next steps for multi-project deployments (cross-project `ref`, project-level namespacing, patterns for development & deployment). Continue the story around stream processing (materialized tests, managed sources). Integrating dbt metrics and MetricFlow. | 75% |
| 1.7 | October | More on the same themes. The details will be based on velocity, feedback, and emergent discoveries. | 50% |
| 1.8+ 💡 | 2024 | dbt-core as a library. A sketch of dbt v2. | 25% |


`updated_at: 2023-02-28`

As always, to keep track of what's happening between these roadmap updates:
- [Milestones](https://github.com/dbt-labs/dbt-core/milestones)
- [GitHub discussions](https://github.com/dbt-labs/dbt-core/discussions)
- [Company blog](https://blog.getdbt.com/) & [dev blog](https://docs.getdbt.com/blog)

Don't forget to ~~like and subscribe~~ [upgrade](https://docs.getdbt.com/guides/migration/versions)!

# Commentary

Let's keep it brief!

## A Python API for dbt-core

dbt Core v1.5 will include:
- A new CLI, based on `click`, with improved help text & documentation
- Support for programmatic invocations, via a Python API, at parity with CLI functionality

Is this it? I don't think so. We have a longer-term vision of dbt-core as a mature software library, with clear interfaces and plugin points.

We aren't going to get all the way there by April. We will have a subset of capabilities that will enable a number of cool things for many. I believe we will be able to get there, over the next year, with carefully scoped initiatives tied to clear outcomes. We've been developing & sharing our visions as a team, and we'll have more to share over the coming months.

_Read more: ["dbt-core as a library: first steps"](https://github.com/dbt-labs/dbt-core/issues/6356)_

## Multi-project deployments (v1.5+)

Here are three guiding principles:
1. Each team owns its data, and how that data is shared with other teams.
2. Organizations can maintain central governance, coordinating rules across teams.
3. All models are in one DAG.

These are not necessarily the doctrine of "dbt mesh," but we are using them to describe the end state we're hoping to achieve, the core capabilities we need to unlock it, and the user flows (person-to-person, team-to-team, person-to-dbt) we want to facilitate along the way.

Each person interacts with the subset(s) of the DAG relevant to them. Developing and deploying dbt should feel the same.

The first step here is giving teams maintaining dbt projects the tools to start serving models as "APIs." The coup de grâce is being able to `ref` another team's stable, public, contracted model as the starting point for your own.

_Read more: ["Multi-project deployments"](https://github.com/dbt-labs/dbt-core/discussions/6725) & linked discussions_

## Support for streaming

Back in 2020, one of Jeremy's very first projects, as newly designated Associate Product Manager, was investigating the implementation of Materialized Views across our most popular data platforms. The findings: while the dream of MVs was a happy one, every real MV was unhappy in its own way, motivated by different use cases and beset by subtle limitations.

A few years later, the major data platform vendors are taking another swing at first-class support for streaming transformation. We're also lucky to have Florian, who talked & thought streaming databases for a living. Our vision is a dbt DAG that can combine batch & streaming, without distorting the core framework that's gotten dbt where it is.

_Read more: ["Let's add Materialized View as a materialization, finally"](https://github.com/dbt-labs/dbt-core/issues/6911)_

## dbt Semantic Layer

We've officially welcomed many new colleagues from Transform. We're going to be spending time over the next several weeks talking about how to integrate dbt Core's existing metrics spec with MetricFlow's.

We expect to be writing much more about this in public. Until then, you can read our previous thinking. The specifics are liable to change, but the foundational concept still holds: ["dbt should know more semantic information"](https://github.com/dbt-labs/dbt-core/discussions/6644)

## What's **not** here?

In 2023, we need to be focused & disciplined. There's a lot we wish we could be making progress on, but we can only guarantee that progress in a precious few areas, by devoting our attention & energy to them.

Each of the topics below has appeared in the lower-confidence portions of previous roadmaps, and they continue to interest us greatly. We have all been guilty of saying, "Let's just take a day—just one day!—and try to hack together a working demo." You might even see some proof-of-concept code appear, here or there. We won't turn down opportunities to take small steps, to make incremental forward progress. But none of these is something we expect us to be launching at Coalesce in October.

- **External orchestration.** Can dbt trigger external APIs to ingest `sources`, and sync `exposures`? To run `models` that require tools outside the data platform? I'd like the answer here to be "yes," but it isn't a priority for this year.
- **Next steps for Python models.** It's worth restating: This is very new functionality, and we're still very early. We have some ideas of what could be compelling and ergonomic, and there are some small usability improvements we'll try to make over the year—but we need to learn more about how you all are using, and want to be using, Python models, before we once again tackle these as a top priority.
- **More modeling languages**, or "Bring Your Own SQL transpiler / Python framework / ???". This is one of the more boundary-pushing ideas we've had, for continuing to expand dbt's reach as a framework for (language-agnostic!) data transformation. It's not one of the foundational reinvestments that we must make sooner rather than later. We've also [discussed this pattern](https://github.com/dbt-labs/dbt-core/discussions/4458#discussioncomment-4176217) as one potential path toward unlocking **column-level lineage,** which—while always present in our hearts & among our [most popular discussions](https://github.com/dbt-labs/dbt-core/discussions?discussions_q=sort%3Atop+)—also doesn't appear on this year's list of top priorities.
- **Unit testing.** ~Let's just take a day—just one day!—and try to hack together a working demo.~ (We might, though.)

## What we'll keep doing

This covers the big rocks. The pebbles and the sand, we already have our ~~mouths~~ hands full. Most of the time, it's even fun.

We'll keep releasing patches with fixes for bugs and any regressions that crop up in new versions of dbt-core.

We'll keep a swimlane open for developer ergonomics. Not fundamental changes to the dbt framework, but quality-of-life improvements for those who use it every day. We've created [a new label to track these "paper cuts"](https://github.com/dbt-labs/dbt-core/issues?q=is%3Aissue+is%3Aopen+label%3Apaper_cut+sort%3Areactions-%2B1-desc)—often among the most upvoted issues!—and we're very interested in supporting community members who want to help us refine & contribute these improvements.

We'll keep reading & responding to your issues, bug reports, feature requests, ideas. We can't respond to every comment everywhere, but we read them all. You come to dbt, and so we build it & keep building it—with ambition & with vision, with discipline & with focus.

Yours truly - Jeremy, Florian, Doug, & the entire Core team
