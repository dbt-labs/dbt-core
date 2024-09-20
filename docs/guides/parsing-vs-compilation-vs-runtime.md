# Parsing vs. Compilation vs. Runtime

## Context: Why this doc?

There’s a lot of confusion about what dbt does at parse time vs. compile time / runtime. Even that separation is a relative simplification: parsing includes multiple steps, and while there are some distinctions between "compiling" and "running" a model, the two are **very** closely related.

It's come up many times before, and we expect it will keep coming up! A decent number of bug reports in `dbt-core` are actually rooted in a misunderstanding of when configs are resolved, especially when folks are using pre/post hooks, or configs that alter materialization behavior (`partitions`, `merge_exclude_columns`, etc).

So, here goes.

## What is "parsing"?

**In a sentence:** dbt reads all the files in your project, and constructs an internal representation of the project ("manifest").

To keep it really simple, let’s say this happens in two steps: "Parsing" and "Resolving."

### Parsing

As a user, you write models as SQL (or Python!) + YAML. For sake of simplicity, we'll mostly consider SQL models ("Jinja-SQL") with additional notes for Python models ("dbt-py") as-needed.

dbt wants to understand and define each SQL model as an object in an internal data structure. It also wants to know its dependencies and configuration (= its place in the DAG). dbt reads your code **for that one model,** and attempts to construct that object, raising a **validation** error if it can’t.

<details>
<summary>(Toggle for many more details.)</summary>

- (Because your SQL and YAML live in separate files, this is actually two steps. But for things like `sources`, `exposures`, `metrics`, `tests`, it’s a single pass.)
- dbt needs to capture and store two vital pieces of information: **dependencies** and **configuration**.
    - We need to know the shape of the DAG. This includes which models are disabled. It also includes dependency relationships between models.
    - Plus, certain configurations have implications for **node selection**, which supports selecting models using the `tag:` and `config:` methods.
- Parsing also resolves the configuration for that model, based on configs set in `dbt_project.yml`, and macros like `generate_schema_name`. (These are "special" macros, whose results are saved at parse time!)
- The way dbt parses models depends on the language that model is written in.
    - dbt-py models are statically analyzed using the Python AST.
    - Simple Jinja-SQL models (using just `ref()`, `source()`, &/or `config()` with literal inputs) are also [statically analyzed](https://docs.getdbt.com/reference/parsing#static-parser), using [a thing we built](https://github.com/dbt-labs/dbt-extractor). This is **very** fast (~0.3 ms).
    - More complex Jinja-SQL models are parsed by actually rendering the Jinja, and "capturing" any instances of `ref()`, `source()`, &/or `config()`. This is kinda slow, but it’s more capable than our static parser. Those macros can receive `set` variables, or call other macros in turn, and we can still capture the right results because **we’re actually using real Jinja to render it.**
        - We capture any other macros called in `depends_on.macros`. This enables us to do clever things later on, such as select models downstream of changed macros (`state:modified.macros`).
        - **However:** If `ref()` is nested inside a conditional block that is false at parse time (e.g. `{% if execute %}`), we will miss capturing that macro call then. If the same conditional block resolves to true at runtime, we’re screwed! So [we have a runtime check](https://github.com/dbt-labs/dbt-core/blob/16f529e1d4e067bdbb6a659a622bead442f24b4e/core/dbt/context/providers.py#L495-L500) to validate that any `ref()` we see again at compile/runtime, is one we also previously captured at parse time. If we find a new `ref()` we weren’t expecting, there’s a risk that we’re running the DAG out of order!

</details>

### Resolving

After we’ve parsed all the objects in a project, we need to resolve the links between them. This is when we look up all the `ref()`, `source()`, `metric()`, and `doc()` calls that we captured during parsing.

This is the first step of (almost) every dbt command! When it's done, we have the **Manifest**.

<details>
<summary>(Toggle for many more details.)</summary>

- If we find another node matching the lookup, we add it to the first node’s `depends_on.nodes`.
- If we don’t find an enabled node matching the lookup, we raise an error.
    - (This is sometimes a failure mode for partial parsing, where we missed re-parsing a particular changed file/node, and it appears as though the node is missing when it clearly isn’t.)
- Corollary: During the initial parse (previous step), we’re not actually ready to look up `ref()`, `source()`, etc. But during that first Jinja render, we still want them to return a `Relation` object, to avoid type errors if users are writing custom code that expects to operate on a `Relation`. (Otherwise, we’d see all sorts of errors like "NoneType has no attribute "identifier.") So, during parsing, we just have `ref()` and `source()` return a placeholder `Relation` pointing to the model currently being parsed. This can lead to some odd behavior, such as in [this recent issue](https://github.com/dbt-labs/dbt-core/issues/6382).

</details>

## What is "execution"?

**In a sentence:** Now that dbt knows about all the stuff in your project, it can perform operations on top of it.

Things it can do:

- tell you about all the models that match certain criteria (`list`)
- compile + run a set of models, in DAG order
- interactively compile / preview some Jinja-SQL, that includes calls to macros or ref’s models defined in your project

Depending on what’s involved, these operations may or may not require a live database connection. While executing, dbt produces metadata, which it returns as **log events** and **artifacts**.

Put another way, dbt’s execution has required inputs, expected outputs, and the possibility for side effects:

- **Inputs** (provided by user): project files, credentials, configuration → Manifest + runtime configuration
- **Outputs** (returned to user): logs & artifacts
- **Side effects** (not seen directly by user): changes in database state, depending on the operation being performed

### Compiling a model

We use the word "compiling" in a way that’s confusing for most software engineers (and many other people). Most of what’s described above, parsing + validating + constructing a Manifest (internal representation), falls more squarely in the traditional role of a language compiler. By contrast, when we talk about "compiling SQL," we’re really talking about something that happens at **runtime**.

Devils in the details; toggle away.

<details>
<summary>The mechanism of "compilation" varies by model language.</summary>

- **Jinja-SQL** wants to compile down to "vanilla" SQL, appropriate for this database, where any calls to `ref('something')` have been replaced with `database.schema.something`.
- dbt doesn’t directly modify or rewrite user-provided **dbt-py** code at all. Instead, "compilation" looks like code generation: appending more methods that allow calls to `dbt.ref()`, `dbt.source()`, and `dbt.config.get()` to return the correct results at runtime.

</details>

<details>
<summary>If your model’s code uses a dynamic query to template code, this requires a database connection.</summary>

- At this point, [`execute`](https://docs.getdbt.com/reference/dbt-jinja-functions/execute) is set to `True`.
- e.g. `dbt_utils.get_column_values`, `dbt_utils.star`
- Jinja-SQL supports this sort of dynamic templating. dbt-py does not; there are other imperative ways to do this, using DataFrame methods / the Python interpreter at runtime.

</details>

<details>
<summary>Compilation is also when ephemeral model CTEs are interpolated into the models that `ref` them.</summary>

- The code for this is *gnarly*. That’s all I’m going to say about it for now.

</details>

<details>
<summary>When compiling happens for a given node varies by command.</summary>

- For example, if one model’s templated SQL depends on an introspective query that expects another model to have already been materialized, this can lead to errors.
- In `dbt run`, models are operated on in DAG order, where operating on one model means compiling it and then running its materialization. This way, if a downstream model’s compiled SQL will depend on an introspective query against the materialized results of an upstream model, we wait to compile it until the upstream model has completely finishing running.

</details>

</br>

The outcome of compiling a model is updating its Manifest entry in two important ways:
- `compiled` is set to `True`
- `compiled_code` is populated with (what else) the compiled code for this model

### Running / materializing a model

A model’s `compiled_code` is passed into the materialization macro, and the materialization macro is executed. That materialization macro will also call user-provided pre- and post-hooks, and other built-in macros that return the appropriate DDL + DML statements (`create`, `alter`, `merge`, etc.)

(For legacy reasons, `compiled_code` is also available as a context variable named [`sql`](https://github.com/dbt-labs/dbt-core/blob/16f529e1d4e067bdbb6a659a622bead442f24b4e/core/dbt/context/providers.py#L1314-L1323). You'll see it referenced as `sql` in some materializations. Going forward, `model['compiled_code']` is a better way to access this.)

## Why does it matter?

Keeping these pieces of logic separate is one of the most important & opinionated abstractions offered by dbt.

- **The separation of "control plane" logic** (configurations & shape of the DAG) **from "data plane" logic** (how data should be manipulated & transformed remotely).
    - You must declare all dependencies & configurations ahead of time, rather than imperatively redefining them at runtime. You cannot dynamically redefine the DAG on the basis of a query result.
    - This is limiting for some advanced use cases, but it prevents you from solving hard problems in exactly the wrong ways.
- **The separation of modeling code** ("logical" transformation written in SQL, or DataFrame manipulations) **from materialization code** ("physical" state changes via DDL/DML)**.**
    - Every model is "just" a `select` statement (for Jinja-SQL models), or a Python DataFrame (for dbt-py models). It can be developed, previewed, and tested as such, *without* mutating database state. Those mutations are defined declaratively, with reusable boilerplate ("view" vs. "table" vs. "incremental"), rather than imperatively each time.


## Appendix

<details>
<summary>Click to toggle notes on parsing</summary>

### Notes on parsing

- **dbt has not yet connected to a database.** Every step performed thus far has required only project files, configuration, and `dbt-core`. You can perform parsing without an Internet connection.
- There is a command called `parse`, which does **just** "parsing" + "resolving," as a way to measure parsing performance in large projects. That command is the fastest way to write `manifest.json` (since v1.5).
- In large projects, the parsing step can also be quite slow: reading lots of files, doing lots of dataclass validation, creating lots of links between lots of nodes. (See below for details on two potential optimizations.)

### Two potential optimizations

1. [**"Partial parsing."**](https://docs.getdbt.com/reference/parsing#partial-parsing) dbt saves the mostly-done Manifest from last time, in a file called `target/partial_parse.msgpack`. dbt **just** reads the files that have changed (based on file system metadata), and makes partial updates to that mostly-done Manifest. Of course, if a user has updated configuration that could be relevant globally (e.g. `dbt_project.yml`, `--vars`), we have to opt for a full re-parse — better safe (slow & correct) than sorry (fast & incorrect).
2. [**"Reusing manifests."**](https://docs.getdbt.com/reference/programmatic-invocations#reusing-objects) Note that this is taking "full control," and there are failure modes (example: [dbt-core#7945](https://github.com/dbt-labs/dbt-core/issues/7945)).

</details>
