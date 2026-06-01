# Let's talk about dbt

Any writer, any artist, who finds themselves doing work that invites [thoughtful, clear, forward-looking](https://thecreativeindependent.com/people/doreen-st-felix-on-entering-the-world-of-criticism) criticism should be counted lucky. As a person who builds dbt Core for a living, I feel lucky to have [Pedram](https://pedram.substack.com/p/we-need-to-talk-about-dbt). He's a person who understands exactly what dbt is, and who cares deeply about its future.

It's possible that we're not building the right things. It's certain that we're not doing a good enough job of communicating what we're building. How can we expect to know the former without doing the latter? In the end, [transparency always wins](https://www.getdbt.com/dbt-labs/values/#:~:text=Transparency%20always%20wins.).

### What is this doc?

This is my story of dbt Core in 2022: everything we're trying to build, and why.

This Markdown table is your `tl;dr`. The rest is commentary.

| Version | When | Stuff | Confidence* |
|-----|----------|----------------------------------------------------------------------------------|------------|
| 1.1 ✅ | April | Testing framework for dbt-core + adapters. Tools and processes for sustainable OSS maintenance tools. | 100% |
| 1.2 | July | Refactoring core materializations (especially incremental). Built-in support for grants. "Cross-db" macros in dbt-core + plugins. Python-language models (beta). | 95% |
| 1.3 | October | Python-language models. Built-in support for UDFs. Exposures / external nodes. | 80% |
| 1.4 | Jan 2023 | Mechanisms for cross-project lineage (namespacing). Performance tune-up. Start incorporating SQL grammar (linting & lineage). | 50% |

`updated_at: 2022-04-12`

**\*Confidence?** I mean the certainty of which things we build for which release. dbt Core is relied on by many. The most important things stay the same, but some of the details vary. When looking 6+ months into the future, I expect us to do ~half of the things I've listed above and below, and shuffle ~half of them out in favor of the things that you will be telling us we need to build.

### Why this doc?

A product philosophy of dbt, taking inspiration [from Perl](https://www.amazon.com/gp/feature.html?ie=UTF8&docId=7137#:~:text=%22Easy%20things%20should%20be%20easy%2C%20and%20hard%20things%20should%20be%20possible.%22): **"Make the easy things easy, and the hard things possible."**

The analyst in you probably has some questions:
- Which things? The entire analytics engineering workflow?
- What ought to be easy? What deserves to be hard?
- Who's making them, and when?

I haven't been doing a good enough job of answering those questions, in a single easy-to-bookmark place. I don't do my long-form writing in a personal blog, I do it [in a GitHub repo](https://github.com/dbt-labs/dbt-core/issues?q=is%3Aissue+is%3Aopen+commenter%3Ajtcohen6+), and trawling through issues is a leisure activity enjoyed by precious few.

dbt Core is a product, sort of; a CLI tool, for many; a software project, for sure; an open source project, definitively; but above all it's a guide, a set of opinionated practices about how you should do this work of data transformation, testing, documentation. We believe these are many (not all) of the essential components of the thing we call analytics engineering, which maybe you just call getting your job done. If you're reading this, even if you don't pay to use dbt, you've more than likely invested in it a meaningful quantity of your time, care, and attention—not to mention, your business logic. It matters a lot to me that you know how we're thinking about dbt's ongoing development. (That also means that the story I can tell in this document is necessarily incomplete: I can tell you what dbt Core can do for you, but just as important is [what you and other skilled practitioners can do with dbt Core](https://docs.getdbt.com/blog).)

That is to say, _mea culpa_. The best time to have written this document was a few months ago; the second best time, I hope, is right now.

### Where are we now?

dbt is a workflow tool. It was originally built by a team of full-time analytics consultants (us). We built it because there were jobs to be done, literally, and dbt made us unreasonably effective at doing them. As a fresh-faced Data Analyst, with a seat in the engine room (the only room there was), I was able to deliver high-quality work to clients ahead of schedule, [and go home early](https://www.getdbt.com/dbt-labs/values/#:~:text=We%20work%20hard%20and%20go%20home.) (or to the beach).

The people who built dbt had the same names, faces, and hand gestures as the people who used it every day. That was our blessing and our curse. Each time we found a new idiosyncrasy in analytical databases, we coded it into dbt. Late-bound views on Redshift? Always. Quoted identifiers on Snowflake? Never. These turned into magic incantations, whose full meaning could only be revealed, slowly, through immersion in our shared ritual practice. To the uninitiated, the curly-braced characters of the incremental materialization may more resemble cuneiform than Python.

As an end user, `dbt-core` felt like a magical experience, even as it grew to be an indecipherable mess of a codebase. Logging was random. Metadata was at once invaluable and totally undocumented. Our automated testing and release processes were "prayers up" undertakings. This makes sense for where our priorities were. It also meant that, at the start of 2021, two things were true:
1. `dbt parse` was really, really freaking slow for any project with more than 500 models in it.
2. dbt Core was major-version-zero software, a fundamentally unstable substratum, used in production by thousands of people.

Those were our two top priorities in 2021. Speed up dbt-core parsing, especially in development. Work up to v1.0, and release it. We made steady progress on both of those, not without some public pains along the way. There were moments when I, like Phoebe, wondered if we'd make it through December. We did. [Read all about it, featuring a dorky picture of me, and my genuine gratitude to all the colleagues and community members who made it happen](https://www.getdbt.com/blog/dbt-core-v1-is-here/).

Those were two big accomplishments. (We also grew the team of people working full-time on dbt Core from three to eight.) That doesn't mean we're done developing Core, though—not even close. I reprised the exercise. At the start of 2022, three things were true:
1. **There are easy things we need to make easy, and one hard thing we need to make possible.** Users of dbt are still totally constrained to what they can write in SQL (or Jinja-SQL). At the same time, there's still too many things that, while doable, require lots of insider knowledge, custom code, or magic incantations. dbt Core needs some **new constructs,** a mix of totally-new and just-plain-easier things.
2. **dbt-core is still missing the right modular interfaces for long-term development.** This is a codebase that grew organically over time. Its layout today is _much_ more sensible than it was a year ago, but we're not all the way there. The tight couplings, of tasks with configs with CLI initialization, tax our ability to build next-level capabilities with confidence, including all the stuff in (1). Not just that: it prevents community members and technology partners from building truly next-level integrations. dbt Core needs some **new interfaces.**
3. **And yet: dbt Core is major-version-one software.** There are commitments to backwards compatibility and ease of upgrade that we take incredibly seriously. This is a necessary constraint that's always top-of-mind for us when building the new (1) and refactoring the old (2).

I believe those three concerns are and remain relevant for every user of dbt. There are going to be specific things we build along the way that might be less relevant for you as an individual—support for other databases, if you happily use Redshift/Snowflake/BigQuery; support for large projects, if you're at a happy medium; better programmatic interfaces, if you need only to invoke dbt from the CLI—but they all weave together into the larger story we're telling this year.

There's inevitably some "inside dbt-ball" in this document. It can't just be a list of user-facing features we're shipping, or I wouldn't be doing the story justice. We're here to take big swings, the kinds of things I get excited about demo'ing at Staging and Coalesce—but we're also here to maintain critical infrastructure, to grow a roster of talented position players who can put in a solid nine innings day after day. So this will also discuss some behind-the-scenes work we're doing, and the people who rely on it.

It's true that many of the people now building dbt aren't themselves users of it—they're talented software engineers who know the things we need to do to scale large, performant, maintainable codebases. It's my job, and the job of my long-time colleagues, to ensure that dbt continues to feel dbtonic along the way.

# Program for the 2022 season

Welcome to the official program for "dbt: A Core Story," the devised theatre piece where everyone has a part to play. First, a few housekeeping items.

We'll be releasing a new minor version of dbt Core every 3 months: April, July, August, January (2023). We put out our new [adapter](https://docs.getdbt.com/docs/available-adapters) versions at the same time. Adapters maintained by other people might be available for upgrade a few weeks later. For each new minor version, we put up a ["migration guide"](https://docs.getdbt.com/docs/guides/migration-guide) in the docs.

Everything we work on is public on GitHub. I attach issues to [milestones](https://github.com/dbt-labs/dbt-core/milestones) for upcoming releases. We're using [discussions](https://github.com/dbt-labs/dbt-core/discussions) now for bigger-picture conversations—stuff we're thinking about, even if we can't write the code for it right away. If the dbt Community Slack feels overwhelming, discussions still feel, for now, like a slower-paced forum for dreaming up ideas, talking through caveats and rough edges. I really welcome your voices there.

As for this document: Each quarter, I'll swing back and update it, with the edits preserved in git. If you see something you don't like, you'll know whom to `blame`.

## Act I: Sharpening our tools (January–April)

### Scene 1: Rewrite the way we test dbt-core and adapters

This probably isn't news to you: dbt-Jinja code is pretty hard to test. I'm not talking about tests for data quality, but tests of "application code," in the sense of making sure that the incremental materialization is doing the right thing in the right circumstances. We've been feeling the pain here, too—it's really hurt our ability to onboard new engineers to the team, and ship features quickly and confidently. It's also hurt our ability to help the people who, whether for their job or with the generosity of their free time, maintain the adapter plugins that let dbt talk to dozens of databases. As core maintainers, we owed them a lot more than we could give them.

So, we wrote a new testing framework, based in `pytest`, that's much easier to extend and debug. Who benefits? Concretely, these automated tests offer us a way to stamp our approval on adapter plugins developed and maintained outside dbt Labs, while ensuring a consistent baseline experience, whichever adapter you use. I think everyone who wants healthy competition between database vendors, and to know that you can have the same magical experience of using dbt across them. Ask your neighborhood adapter maintainer if the new testing framework is working well for them.

Is the new `pytest`-based framework actually the dbt feature called "unit testing" in disguise? Funny you asked—no, but I do think it's a step on the way there. It's possible to use this functional testing framework to quickly spin up "projects," with fixture inputs and outputs, that check the behavior of a macro. We can test that macro for consistency across many databases. (Check this out in that most complex project of them all, [`dbt_utils`](https://github.com/dbt-labs/dbt-utils/pull/588).) The dream is indeed to expose a dbt-code-only version of this workflow, along the lines of what people have been cooking up in https://github.com/dbt-labs/dbt-core/discussions/4455.

(We also intentionally built this in a way that avoids locking us into Jinja-y code forever. Jinja is a tool, just like Python. It gets us a lot right now. There will come a time when it's no longer the right fit, and we'll need to be ready to move away from it.)

### Scene 2: Sustainable maintainership

Since December, we've added three new folks to the team. There's 10 of us now! You may have seen some newer names, if you're used to hanging around our GitHub: @leahwicz @gshank @nathaniel-may @iknox-fa @emmyoop @McKnight-42 @VersusFacit @ChenyuLInx @stu-k

That growth was in parallel to the growth of `dbt-core` active users, and the growth of issues in the `dbt-core` repo. We get 2 or 3 new open source issues every calendar day. When it was just me responding, I would take a week of vacation, and come back to a stack of a dozen or more.

At the same time, I was learning a **ton** about dbt—what it is today, what it should be—by reading and responding to all your issues. That's invaluable stuff for the people building dbt, and it's stuff that was all living in my head only.

So, for both reasons, I've been sharing the load with the engineering team building dbt Core. This requires more time, and process, but it also requires a level of rigor and organization that I didn't have when it was just me. Bugs don't get lost in the shuffle, and get resolved more quickly.

Is this groundbreaking stuff? No, but it is major-version-one stuff. Is every one of these issues game-changing? No, but they matter a lot to the person who opens them—and I believe it matters a lot to you, a person who has invested meaningfully in standardizing on dbt Core. You should want to know that that core dbt functionality is well maintained, to know that when you run into a bug or have an idea there will be a human on the other end to receive and respond to it. To that end, I want to share a new doc we put together over the past few months: [Expectations for OSS Contributors](https://docs.getdbt.com/docs/contributing/oss-expectations).

It took some doing, but we now live in a world where:
- every issue gets a timely response
- every community PR gets code review (with easier code checks! and conflict-free changlog entries!)
- they're not all (or even mostly) from Drew or me

I care about making contribution accessible, and making dbt a thing we can keep building together. I care a lot about what you have to say and think about feel about dbt. I still read every new issue, even if it's not me who responds. Maybe I'll stop, someday. But for now I read them all.

### Scene 3: v1.1 (Gloria Casarez)

At the end of April, [we released v1.1](https://github.com/dbt-labs/dbt-core/releases/tag/v1.1.0), named for Gloria Casarez, the activist and advocate for the rights of LGBTQ+ people in Philadelphia. This release had [some good stuff in it](https://docs.getdbt.com/docs/guides/migration-guide/upgrading-to-v1.1), including a few boundary-pushing community contributions—but I'll be the first to admit that it was a lighter release than others in recent memory. The biggest work was sharpening our tools, and sharpening the team. We'd rather do this work upfront, so we can build and ship the bigger stuff, with alacrity and assurance.

## Act II: Ergonomic interfaces (May–July)

This is where we are now. There are three big focus areas for the Core team in these three months. Two of them are user-facing, and one of them is software-facing. This is work we've just kicked off, and if you choose to look closely, you can see the full flurry of issues and PRs flying around.

### Scene 1: "Adapter ergonomics"

For the full story, and to leave your thoughts, check out the discussion: https://github.com/dbt-labs/dbt-core/discussions/5091. Meanwhile, I'll give the quick & dirty version.

**First, grants.** These have been, [in Tristan's words](https://roundup.getdbt.com/p/the-response-you-deserve), "super-budget literally forever." Database permissions matter; they're an easy thing, and yet dbt makes them hard. I've lost track of the number of issues (but linked to many of them in https://github.com/dbt-labs/dbt-core/issues/5090) where users have run into bad experiences with `pre-hook` and `post-hook`, which are [rendered a little bit differently](https://docs.getdbt.com/docs/building-a-dbt-project/dont-nest-your-curlies), in ways that can be very useful and very confusing. Honestly, we want to move hooks into the category of "advanced functionality"—use only if you must!—but we can't do that while `grants` remain hooked on hooks.

There's another reason to do this work now: BigQuery added support for DCL statements [last summer](https://cloud.google.com/bigquery/docs/release-notes#June_28_2021). Databricks has built a [brand-new unity catalog](https://databricks.com/product/unity-catalog). Many warehouses are adding more-complex permissions—dynamic data masking, role-based access policies on rows and columns. If dbt can be a forcing function for baseline consistency among data warehouse vendors—make the easy stuff easy—it provides a foundation, and clears human brain space, for the trickier case-dependent capabilities.

**Second, materializations:** specifically, the 'incremental' materialization, and the sorta-real thing that is "incremental strategy" on a handful of adapters. These are powerful, complex, misunderstood. We need more sensible code, more sensible docs. Lots of people (adapters, users) want to customize or contribute to materialization logic. It's really tricky to write today, in ways that go beyond the standard (fair) complaints about macros. Our functional testing framework helps here, by giving us the ability to define the superset of behaviors in `dbt-core`, and to let each adapter or user opt into and fine-tune the ones they wish to support.

**Third, `dbt-utils`**, which is getting unsustainably big. As part of [the work of splitting it up](https://github.com/dbt-labs/dbt-utils/discussions/487), we decided that its lowest-level building blocks—the "cross-db" macros so useful to authors of other packages—really belong in `dbt-core` and plugins. This is an opportunity for us to continue expanding the "dbt-SQL" language, such as it exists today, for the benefit of the package maintainers who contribute so much to our ecosystem.

**The v1.2 release** will include all of the above, with a beta prerelease in June, and a final releaes to arrive in mid-July. What else? v1.2 will also to include [support for ratio metrics](https://github.com/dbt-labs/dbt-core/issues/4884), to power dbt's rapidly iterating semantic layer. Last and never least, it will include the dozen or so capabilities for which community members have taken the initiative to contribute. (Fun fact: thanks to our new `changie`-powered changelog, you can always catch a glimpse of [upcoming changes](https://github.com/dbt-labs/dbt-core/tree/main/.changes/unreleased), merged but not yet released.)

### Scene 2: Modular programmatic interfaces

If you've hung around the `dbt-core` GitHub repo lately, you may have seen that we've started tagging a lot of issues with `tech_debt`, as well as with `Team:Language`, `Team:Execution`, `Team:Adapters`.

Not long ago, we had one team (one person), one codebase, one tightly coupled application that did all of the things, from beginning to end, without clear stopping points in the middle. Now, I'm truly lucky to work with a team of talented software engineers, as we try to make the experience of developing and interfacing with `dbt-core` as no-bs as it is to use. We believe the highest-leverage way to do this is by splitting up the team into specialties, and the codebase into modular interfaces.

For too long now, the [docs for dbt's "Python API"](https://docs.getdbt.com/docs/running-a-dbt-project/dbt-api) have cautioned:

> It _is_ possible to import and invoke dbt as a Python module. This API is still not contracted or documented, and it is liable to change in future versions of `dbt-core` without warning. Please use caution when upgrading across versions of dbt if you choose to run dbt in this manner!

There are two big parts of this initiative that I'm hoping to tackle in July, after we've readied v1.2 for prerelease testing:
1. Decouple the CLI from the `dbt-core` "library." Support actual programmatic interfaces into initialization and tasks. Unbundle the massive blobs of config that get passed around during initialization today.
2. **Structured logging**, as the future of (real time) dbt metadata. In v1.0, we replaced all our "legacy" logging with a real event system and structured interface. We need to extend that system to handle errors/exceptions, too—a pain to debug when dbt spits back no or little information—and we need to add much more logging than we have today. There's so much more valuable information that dbt collects _while_ it runs, which we want to expose for you and everyone else. (Think: model table statistics _during_ `dbt run`, not just after `docs generate`.) Eventually, we see these logs eclipsing `manifest.json`—which doesn't go away, exactly, but is no longer the one (giant) file that answers every question about a dbt project.

These things don't pay dividends as quickly as user-facing functionality, but we believe they more than pay for themselves in the long run. Here are the three ways I can think of:
1. Better interfaces makes us faster at onboarding new engineers to the Core team, and building new features in dbt Core.
2. External contributors (you!) can contribute code to `dbt-core` more easily: to find the right module, the right interfaces to extend, and fewer pitfalls that sap enthusiasm while doing it.
3. It's easier to build other tools on top of dbt Core. That includes dbt Cloud—which, candidly, has been hamstrung in its ability to develop differentiated experiences by the lack of reliable interfaces in Core. But it also includes way-cool tools, such as [`sqlfluff`](https://github.com/sqlfluff/sqlfluff) and [`fal`](https://github.com/fal-ai/fal), that already hook into `dbt-core`'s officially undocumented Python interface. Let's make that the practice, not the anti-pattern.

### Scene 3: Python-language dbt models

(!!)

Sorry to bury the lede for this one. This is shaping up to be the groundbreaking new entrant in the Core roadmap in 2022. If do our job well, we'll have made a hard thing possible; if we do it very well, we might just manage to make it feel easy.

Over the past few weeks, I've been drafting thoughts for a forthcoming discussion, and Core engineers have been playing around with a little bit of experimental code. We're hoping to have something that's beta-ready by July. I'll have more to say about this next week, including my personal take on le grand débat of SQL ~versus~ and Python.

For now, here's a taste:

```python
def model(dbt):

    # look familiar?
    dbt.config(
        materialized='table',
    )

    # this knows about the DAG
    upstream_model = dbt.ref("upstream_model")
    upstream_source = dbt.source("upstream", "source")

    # dataframe-style transformations
    sample = upstream_model.where(col("city")=="Philly")
    ...

    # this too
    import numpy as np
    ...
    
    # your final 'select' statement
    df = ...

    # return a dataset, just like SQL — dbt takes care of the rest
    return df
```

Once the beta is available, we'll want to hear everyone's thoughts. Our plan is to incorporate your thoughts, ideas, and feedback, as well as some complementary features we have in mind, as we build toward...

## Act III: Unified Lineage (August–October)

v1.3 takes us to Coalesce, where I might just manage to meet (some of) you in person for the very first time.

The theme of this release will be **unifying lineage.** We're pulling some new pieces of transformation logic into dbt's execution model, and offering more ways to push the rich metadata from your project out into the world.

Things like:
- Python-language dbt models, ready for production
- Native support for UDFs (https://github.com/dbt-labs/dbt-core/issues/136: an oldie, a goodie, and newly relevant!)
- Improvements to exposures, and external nodes, and maybe those are actually the same thing? (https://github.com/dbt-labs/dbt-core/discussions/5073)

What else? 'Tis not so sweet now as it was before? I think there will be lots to discuss, some to debate, things to try out, a few to throw away, and maybe just maybe we'll find ourselves with an expanded standard, a thing we're proud to still call dbt Core.

Thoughts? For my part, I think **unit testing** model code becomes an even more pressing question when:
- dbt models can be written in Python, a language that _wants_ to be unit-tested even more than SQL
- dbt nodes (models?) can return _functions_, with testable inputs and outputs
- dbt can be interacting with external services that live outside the database

I rest well knowing we'll be better equipped to solve that problem, using all that we learn about Python-language models, and the components of the `pytest` framework that we put together in the first few months of the year.

Of course, all of that will be in addition to the ongoing work that we're doing around metrics and the semantic layer, first previewed at last year's Coalesce. It's not my place to spoil any of the surprises there.

## Act IV: Reflect and reinvest (November–January)

v1.3 takes us through Coalesce, which is always a bit of an event horizon for the Product team at dbt Labs. Looking further than six months away, I'm only ~50% certain about which initiatives we'll be tackling into the new year. For now, I can share the ones that feel most important to me:

**Better mechanisms for cross-project lineage** (https://github.com/dbt-labs/dbt-core/discussions/5244), to support large projects that should really be multiple projects. That would include support for namespaced models, at least in different packages. This is an initiative I care about a lot, and I'm trying to push up earlier if possible.

**Building a SQL grammar**, or build compelling features on top of another open source one. Catch syntax errors at parse time, lint code, auto-format. Also, detect column-level lineage. This gets complicated, because of how some dbt models are templated dynamically, but I trust that we'll be able to find reasonable compromises when people are doing more-complex things to template model SQL.

**Revisiting performance.** It will have been 2 years since we first began performance work in earnest, in November 2020. Then, we had the goal of speeding up dbt startup by 100x. Now, we should see where the bottlenecks are—I sense they're around database caching and cataloging. Then, we chose to write some low-lying parts of dbt-core in Rust, instead of Python. Now, with more clearly defined interfaces, it might make sense to rewrite some parts again.

## Epilogue: What's missing?

It's not all accounted for above. There are many important things we won't be able to build. I can name a few; I'm sure you have more than these, and I want to hear them.

**Meaningfully rebuilding, rewriting, or reinvesting in the `dbt-docs` site.** We're going to keep this up & running, and we'll review community PRs—but we haven't built a team of Angular (!) engineers. The fact that `dbt-core` ships with an auto-generated documentation site is essential; the next-level version of this thing needs to be built on a more powerful engine than a static site reading JSON files.

**DRYer configuration.** YAML reuse. Macros in `.yml` files. Vars of vars, and docs blocks on docs blocks. Why not solve it sooner? I think column-level lineage has a part to play here; anything we build before then risks emphasizing the wrong details. But I appreciate the real frustrations around config duplication and code generation in the meantime. dbt can be verbose, not unlike yours truly.

**Native plugins for VS Code, PyCharm, Vim, ...**—I know. I want these things _very_ badly. I think the role of the Core team, first and foremost, is to solidify and harden the programmatic interfaces that I mentioned above. There's no sense building a thing on a rocky (let alone undocumented) foundation. This is also the best way for us to expand our reach: by enabling our colleagues, yes, and also partners, and also every member of the community with a hack day, to build better and cooler stuff, and build it with confidence. It's the kindness of strangers, and then some—it's open source.

**What about `<issue that's been open since ...>`?** Tell me which one, and I'll give it another read. I'm `@jerco` in dbt Community Slack.
