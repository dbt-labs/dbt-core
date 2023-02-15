# dbt Core: So much to say (February 2023)

We're back, and there's so much to say! So much that if we're not mindful, we may end-up in short novela territory in no time. Nobody has the patience to read that, so we will do the opposite this time: minimalism. But we will link to wherever more details can be found for each item!

Since last August, we:
- Released dbt Core v1.3, unleashing Python models onto the world. The adoption has met our expectations, we are still gathering feedback on where to go next - get in touch!
- Released dbt Core v1.4, reworking a lot of the internals, paving the way to a saner experience for contributing to dbt core (for us and all contributors)
- Started working on dbt Core v1.5, continuing the effort started on internals in v1.4, but also getting started on "small" things like multi-project deployments, or streaming support via materialized views. Small stuff. 
- Something about Transform?

As always, to keep track of what's happening between these roadmap updates, the places to be are [the blog](https://www.getdbt.com/blog/), [the other (cooler) blog](https://docs.getdbt.com/blog), and the [GitHub discussions](https://github.com/dbt-labs/dbt-core/discussions). 

Here's what you came for:

| Version | When<sup>a</sup>| Namesake<sup>b</sup>| Stuff | Confidence<sup>c</sup> |
| ------- | ------------- | -------------- | ----- | ------------ |
| 1.1 ✅ | April 2022   | Gloria Casarez | Testing framework for dbt-core + adapters. Tools and processes for sustainable OSS maintenance. | 100% |
| 1.2 ✅ | July 2022    | Henry George | Built-in support for grants. Migrate cross-db macros into dbt-core / adapters. Improvements to metrics. | 100% |
| 1.3 ✅ | October 2022 || Python models in dbt. More improvements to metrics. | 100% |
| 1.4 ✅ | Jan || Behind-the-scenes improvements to technical interfaces. A real, documented Python API/library, with an improved CLI to wrap it. Further investments in structured logging. | 100% |
| 1.5 ⚒️ | May || More internal improvements, the beginning of Multi-project deployments and Materialized views | 95% |
| 1.6 🌀 | Sep || A fuller story around stream processing : materialized tests, managed sources, etc. | 75% |
| 1.7 💡 | Jan 2024 || 2024? That's becoming ridiculous. Is that time for a v2? Or can we keep pushing on v1? | 25% |

`updated_at: 2023-02-15`

<sup>a</sup>We're sticking with one minor version release per quarter, for the foreseeable. I haven't split those out here because, 6+ months into the future, we care more about the _what_ and the _why_ than the _when_. As we get closer, we'll be able to detail the more-specific functionality that might land in specific releases. Note too that these ideas, though we're already devoting meaningful time and effort to thinking through them, are not definite commitments.

<sup>b</sup>Always a [phamous Philadelphian](https://en.wikipedia.org/wiki/List_of_people_from_Philadelphia), true to our roots. If you have ideas or recommendations for future version namesakes, my DMs are open :)

<sup>c</sup>dbt Core is, increasingly, a standard-bearer and direction-setter. We need to tell you about the things we're thinking about, long in advance of actually building them, because it has real impacts for the plans of data teams and the roadmaps of other tools in the ecosystem. We also know that we don't know now everything we will know a year from now. As new things come up, as you tell us which ones are important to you, we reserve the right to pivot. So we'll keep sharing our future plans, on an ongoing basis, wrapped in a confidence interval.

# Commentary

Don't forget to ~~like and subscribe~~ [upgrade](https://docs.getdbt.com/guides/migration/versions).

## v1.5 (May)

If you've been following our GitHub discussions, or the Analytics Engineering roundup, none of these topics should come as too much of a surprise. They're neither definite commitments, nor the full set of things we expect to do next year. There's a lot of linear improvement to existing functionality that's always on our minds, and in our issues. But I want to start with the pair of ideas that we've been talking about nonstop, for which we're already dreaming up some code:

1. **Multi-project deployments.** `ref` a final model from someone else's project, wherever they've put it, without the need to run it first. Split up monolithic projects of 5000 models into 10 projects of 500, grouped by team and domain. This is more than just "namespacing": to really solve for this, we also need to solve for versioning and contracts, and support a variety of deployment mechanisms. The discussion for this has been in [#5244](https://github.com/dbt-labs/dbt-core/discussions/5244); I'll have more to share over the next few months.

2. **External orchestration.** The same dbt DAG, playing a more active role. We've been developing this idea internally, and have arrived at a few strong opinions. This would not be a new node type, but an upgrade to the ones we already have: sources, models, and exposures. Sources that can trigger their own ingest. Exposures that can trigger downstream data consumers (syncs, sinks, etc). Models that can define and run transformations in dedicated execution environments, reading from and writing back to centralized data storage. For each of those external integrations, a simple request where possible, and a dedicated plugin where justified. If you're someone who followed along the original "external nodes" discussion ([#5073](https://github.com/dbt-labs/dbt-core/discussions/5073))—especially if you've got a tool you'd be excited to integrate into dbt's DAG—let's talk.

## v1.6 (September)

## v1.7+ (Next year)

---

This covers the big rocks. The pebbles and the sand, we have our ~~mouths~~ hands full of, and most of the time it's fun.