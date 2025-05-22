# Project Overview

We are contributing a new feature to dbt-core. This will be a real PR shared with the team.

The feature is called a "Concept Ref" and complements the existing dbt concept of a standard `ref()`.

## New Feature Description

Currently, dbt developers use the `ref()` syntax to reference a model.

This is how a dbt model like `fct_orders.sql` might look today:

```sql
select
    orders.order_id,
    orders.status,
    ...
    order_feature_1.delivery_speed,
    order_feature_2.payment_reversal_reason

from {{ ref('stg_orders') }} as orders
left join {{ ref('int_orders_feature_1') }} as order_feature_1
  on orders.id = order_feature_1.order_id
left join {{ ref('int_orders_feature_2') }} as order_feature_2
  on orders.id = order_feature_2.order_id
```

This model joins three upstream models in the dbt project. The `stg_orders` contains the basic 'grain' of "orders" while the other two tables have pre-computed features at the same grain. The entity hsan't changed, it has just been *enriched* by these features. This is THE pattern of a dbt project. A DAG that progressively enhances data models with features calculated in intermediate models.

The New Feature is a new abstraction in the dbt paradigm. Instead of a `ref` we are going to implement a `cref` or a "Concept Ref".

"Concepts" will be defined in a yaml object that describes a pattern of joins. 

For the above example, the concept would be called "orders" and the base of the concept is the grain table `stg_orders` while the joins are the feature tables. The rest of the concept object exists to support the automatic joining of the specified models.

The cref will 'parse' to actual refs.

Here's an example Concept YAML:

```yaml
concepts:
  - name: orders
    description: "some description"
    base_model: stg_orders
    columns:
        - name: order_id
        - name: status
        ...
    joins:
      - name: int_orders_feature_1
        base_key: order_id             # this defaults to the primary key, but is filled in here for clarity
        foreign_key: order_id          # this also defaults to the primary key name, as most projects collide join key column names intentionally.
        alias: of1
        columns:
            - name: order_id
              alias: order_feature_1_order_id  # a unique alias must be provided for colliding column names (or they can be excluded)
            - name: delivery_speed
            ...
      - name: int_orders_feature_2
        alias: of2
        columns:
          - name: payment_reversal_reason
      - name: stg_products
        base_key: product_id
        foreign_key: p_id
        columns:
            - name: p_id
            - name: product_name
            ...
```

The Concept abstraction allows developers to define in YAML of a base model (like `stg_orders`) and its potential joins (`int_orders_feature_1` and `int_orders_feature_2`) as well as the available columns under each join.

Then, in the model SQL, they can simply use a "concept reference" or `cref()` like `{{ cref('orders', ['order_id', 'status', 'delivery_speed', 'payment_reversal_reason']) }}` and the cref will parse to the joins and selection necessary to support the query.

A few basic requirements:

* The joined models must be either 1:1 or M:1 relative to the base table. So `stg_orders` can join to `int_orders_feature_1` or `stg_products` but not `stg_order_items` which would be a 1:M relation.
* The base model must be upstream or unrelated to the feature models. Otherwise every usage would create a DAG cycle.
* The selectable columns must be uniquely named, or provide an alias that is unique in the name space of the entity. So that the list of columns to include does not have ambiguity.


**Key elements of the Concept spec:**

* **`name`:** Unique identifier for the Concept. This is what developers will use in the `cref()` calls.
* **`base_model`:** The core dbt model that the Concept is built on. This is typically a fact or dimension table at the grain of the Concept (e.g. `stg_orders` for an orders Concept). It can be specified with `ref('model_name')` or as a string name (the `ref` will be resolved by dbt).
* **`primary_key`:** The primary key column of the base model that uniquely identifies each Concept record (may be a single column or list of columns). This serves as a default unique/grain indicator and the default foreign key for joins.
* **`features`:** A list of columns (with optional descriptions or expressions) available from the Concept. These typically include the base model’s columns and any additional fields brought in via joins. Each feature can be a simple column name or an object with `name` and optionally an `expr` if the feature is derived (similar to how dimensions can be defined via expressions in semantic layer models). Features from joined models will be exposed here (often under the same name as in the join source, unless aliased).
* **`joins`:** An optional list of join relationships from this Concept’s base to other **models**:

  * Each join specifies a model reference such as `ref('other_model')` or just the model name (e.g., `stg_customers`).
  * **`base_key`:** The column in the base\_model that serves as the foreign key for this relationship.
  * **`foreign_key`:** The column in the joined model that corresponds to the key. If omitted, defaults to the Concept's primary key column name.
  * **`alias`:** (Optional) An alias to use for the joined table in the generated SQL. Defaults to the model name if not provided.
  * **`columns`:** The subset of columns from the joined model to **make available as part of this Concept**. By explicitly listing features, we ensure the `cref` macro knows which columns from the join partner are accessible. These will typically be added to the parent Concept’s feature list (potentially with a prefix or the same name). For instance, in the above example, `customer_name` becomes a feature of `orders` via the join, and `region_id` as well (to allow further chaining or aggregation by region if needed).

**Schema and Docs Integration:** Concept definitions in YAML will be integrated into dbt's documentation generation. Concepts can be documented similar to models (with descriptions, and descriptions on each feature/column). They do not create physical models but should appear in docs as **logical groupings of fields**. This helps users discover which fields are available via an Concept and what they represent.

## `cref` Macro and SQL Compilation Logic

We introduce a new Jinja macro or function, **`cref(Concept_name, field_list)`**, which models will use in their SQL to pull in fields from an Concept. The macro acts as a smarter version of `ref()`: instead of returning a single table, it returns a **subquery or CTE** that includes only the necessary joins to produce the requested fields.

**Usage Example:**

In a model SQL (say `int_order_stats.sql`), a user might write:

```sql
select
    o.order_id,
    o.order_date,
    o.total_amount,
    o.customer_name
from {{ cref('orders', ['order_id', 'order_date', 'total_amount', 'customer_name']) }} as o
where o.order_date >= current_date - interval '30' day
```

Here, `cref('orders', [...])` will compile into a subquery that selects `order_id, order_date, total_amount, customer_name` from the `orders` Concept. Based on the Concept definition, it will generate SQL roughly equivalent to:

```sql
(
    select orders_base.order_id,
           orders_base.order_date,
           orders_base.total_amount,
           customer.customer_name
    from {{ ref('stg_orders') }} as orders_base
    left join {{ ref('stg_customers') }} as customer
      on orders_base.customer_id = customer.customer_id
) as o
```

This output includes only the join to `stg_customers` (via the customer join) because `customer_name` was requested. If we had also requested a product field, the subquery would include a join to `stg_product_details` as well. Conversely, if only base fields were selected, no join would be included at all (just a simple `select` from `stg_orders`). The `cref` macro thereby **dynamically trims upstream joins** to the minimum required set of tables and columns.

**Internal Resolution Process:**

When `cref(Concept, fields)` is called, the compiler will:

1. **Lookup the Concept Definition:** Using the provided `Concept_name`, find the corresponding Concept in the manifest (parsed from YAML). If not found, this is an error (unknown Concept).

2. **Validate and Normalize Field List:** The `fields` argument can be a list of feature names (strings). The compiler checks each field against the Concept’s available features:

   * If a field matches a base\_model column or a feature from one of the defined joins, it is accepted.
   * If a field name is ambiguous (e.g. appears in multiple join sources or conflicts with a base field name), the compiler will raise an error requiring the user to qualify which one they want (this could be resolved by prefix or alias if we support a syntax like `"alias.field"` in the field list).
   * If a field is not found in the Concept’s schema, a compile error is thrown.

3. **Determine Required Joins:** For each requested field, note which source it comes from:

   * If from the base model (including the primary key or any base features), no join needed.
   * If from a joined model, mark that join as required. For example, `customer_name` is provided by the `customer` join in the YAML, so include the `customer` table.
   * If multiple fields come from the same join source, that join is included only once.

4. **Construct the Subquery SQL:** The compiler (within the `cref` macro implementation) generates a SELECT query:

   * **FROM clause:** always start from the base model (`base_model` of the Concept). Use a unique alias (e.g. `orders_base`).
   * **JOIN clauses:** for each required join, add a join to the appropriate model:

     * Each join references a dbt model directly. The join uses that model via `ref()`. For example, a join to `stg_customers` becomes `LEFT JOIN {{ ref('stg_customers') }} AS customer ON orders_base.customer_id = customer.customer_id`.
     * **Join Type:** Default to `LEFT JOIN` unless a different `type` was specified in YAML. Left join is typical to preserve the base rows (especially if base is a fact table and we’re adding dimensional data). In the future, other join types (inner, full) could be allowed via config if needed (for now, left join covers most use cases without dropping base records).
   * **SELECT clause:** include each requested field, qualifying by the appropriate table alias. For base fields, prefix with base alias (or no prefix if unambiguous). For joined fields, prefix with the join alias defined. The macro can automatically alias output columns if necessary to avoid collisions (e.g. if both base and join have a `customer_id` field, one could be aliased).
   * **Column Pruning:** Only the fields requested (plus possibly the primary key) are selected. The primary key of the base might be included implicitly if needed for join logic or to maintain grain integrity, even if not explicitly requested. However, we will not include unnecessary columns.
   * The entire constructed query is wrapped in parentheses (as a subquery) with an alias for use in the outer query. Alternatively, the macro could output it as a CTE definition instead, but wrapping as a subquery inline is simpler and doesn’t require CTE naming. The user can always assign it an alias in their FROM clause (as in `... from {{ cref('orders', [...]) }} as o`).

5. **Return Macro Result:** The `cref` macro returns the constructed SQL string. During compilation, this will be injected into the model's SQL, replacing the `{{ cref() }}` call.

This dynamic compilation ensures that only the **minimal upstream data** is pulled in for a model. If an Concept’s join has many possible features but only one is needed, no other feature tables are touched. Essentially, `cref` performs a kind of **just-in-time join assembly**, following the pre-declared patterns.

## Parser and Compilation Lifecycle Integration

Introducing `cref` requires extending dbt’s parsing and compilation processes. We need the parser to recognize `cref` calls in model SQL and handle them similarly to how `ref` is handled (ensuring dependency tracking). Key integration points:

* **Manifest Structures:** A new structure (e.g. `ParsedConcept`) will be added to dbt's manifest to store Concept definitions from YAML. Each parsed Concept includes:

  * Name, base model reference, primary key, features list, and join definitions (with references to models).
  * These will be stored in the manifest so that during model parsing/compilation, we can quickly look up Concept metadata. They will not appear as Nodes in the DAG (i.e. not as `NodeType.Model`), but possibly as a separate section in the manifest (like how sources and exposures are tracked).

* **YAML Parsing:** The YAML loader will be updated to parse an `Concepts:` section. This is analogous to how sources, metrics, exposures etc. are parsed. The parser will resolve any `ref()` inside `base_model` or `model` fields immediately, linking them to actual model nodes. For example, `base_model: ref('stg_orders')` is resolved to the internal unique identifier of that model in the manifest.

* **`cref` Recognition:** We will implement `cref` as a special Jinja **context function** (similar to `ref`, `source`, etc.), rather than a plain macro. This allows the dbt compiler to intercept calls to `cref` during parse. When the SQL of a model is being parsed:

  * The Jinja rendering context will include an `cref` function that does minimal work: it records the invocation (with the Concept name and list of fields) and returns a placeholder or nothing at parse time. We do **not** want to fully render the SQL at parse (as actual field names or table aliases might not be resolved yet), but we *do* need to capture dependencies.
  * Specifically, when `cref('orders', [...])` is encountered, the parser will:

    * Look up the `orders` Concept in the manifest. If not found, raise a parse error (undefined Concept).
    * Determine which models that Concept might depend on. In the simplest approach, we add a dependency on the Concept’s base model **and all models in its join tree**. However, this could over-add dependencies. A more precise approach is to add dependencies only for the base model and any directly joined models *that are guaranteed to be needed*.
    * At parse time, we don't yet know which specific joins will be needed (because that depends on which fields are selected). We have two options:

      1. Conservative: register dependencies on **all potential upstream models** that the Concept *could* join. This means if `orders` Concept can join `customers` and `products`, the model using `cref('orders', ...)` will be listed as depending on `stg_orders`, `stg_customers`, and `stg_products` in the manifest. This guarantees the DAG is complete (no missing edge if later the compile needs that join). The downside is it may introduce some extra edges (e.g. if the model didn't actually need `products`, it still shows as depending on it). However, since `cref` is optional, users likely won't mind a slightly broader dependency as long as correctness is maintained.
      2. Dynamic parse (advanced): attempt to evaluate the fields argument at parse time (if it’s a static list of literals, which it usually will be) and determine exactly which joins are needed, then only add those dependencies. This is more precise but requires evaluating part of the macro logic at parse time. We could implement a lightweight analysis: check each field name, map it to an Concept or base, and figure out the needed models. This requires the YAML Concept definitions to be accessible during parsing (which they are, having been parsed earlier).
    * For initial implementation, the **conservative approach** is safer: add dependencies on all models referenced by the Concept's base and joins. This ensures no missing dependencies and still avoids creating a standalone Concept node. The DAG impact is that a model using `cref('orders', ...)` will run after `stg_orders`, `stg_customers`, etc., which is correct if any of those fields are used. In cases where not all were needed, the extra dependency might slightly reduce parallelism (e.g. it waits for `stg_products` even if not used), but it preserves correctness and is simpler. We can iterate on this to make it more precise later.
  * The parser will treat these discovered dependencies similar to how multiple `ref()` calls in a model are handled. The model is marked as depending on each relevant upstream model.

* **Compilation Phase:** During the actual SQL compilation of a model (after parsing and graph building), the `cref` function will be invoked again, this time to produce the SQL text:

  * We implement `cref` as a context function that at compile-time performs the **resolution logic** described in the previous section (looking up fields and building SQL). It will call `ref()` on the base model and any joined models *as it generates the SQL*. Because we likely already added those dependencies at parse, these `ref` calls will not introduce unknown new dependencies. (If we went with the dynamic parse approach, we would exactly match needed refs.)
  * The use of `ref()` inside the `cref` expansion is important: it ensures proper schema naming and late-binding (dbt will insert the proper database/schema for the model reference). It also leverages dbt's adapter quoting rules. As a result, the compiled SQL might look as shown (with `{{ ref('stg_orders') }}` replaced by the actual schema and table name).
  * The compilation must also handle any Jinja expressions in the field list or in the YAML (for instance, if an Concept feature is defined by an expression using Jinja or macros, though likely features will be static column names).
  * After compilation, the manifest’s node for this model will have the fully expanded SQL with all joins inlined.

* **Ephemeral Model Parity:** In effect, an `cref` call produces a subquery similar to an ephemeral model. But unlike a user-defined ephemeral model, the Concept join subquery is generated on the fly. We should ensure this doesn't conflict with dbt’s materialization logic:

  * If the base models or joined models are ephemeral themselves (unlikely in most cases, but possible), `ref('ephemeral_model')` returns an inlined CTE. The `cref` expansion would then result in nesting those CTEs inside the subquery. dbt handles multiple ephemeral refs by creating CTEs; similar logic will apply. We might end up with the `cref` subquery containing one or more CTEs for ephemeral dependencies. This should be supported, as dbt can already compile multiple ephemeral dependencies in one query.
  * Concepts themselves have no materialization; they don’t appear in run results. So the `cref` expansion is either part of a model’s single SQL statement or possibly implemented as an **ephemeral node internally** (one could conceptualize that each `cref` invocation spawns an ephemeral node with a unique name that includes the Concept and fields, but since it’s not reused elsewhere in the same query, it's simpler to inline it).
  * For the documentation and lineage, the manifest could record an association that model X uses Concept Y (in addition to the model dependencies). This can be useful for users to understand where Concept logic is used.

In summary, the parser will absorb Concept definitions, and treat `cref` calls somewhat specially to ensure that *all necessary upstream models are included in the dependency graph*. The compilation stage then expands `cref` into actual SQL with refs, piggybacking on dbt's existing compilation and adapter-specific handling.

## Integration with dbt Graph and Manifest

Even though Concepts are not physical nodes, we must reflect their usage in the DAG and manifest:

* **DAG Dependency Graph:** A model using `cref('Concept_name', ...)` will have direct dependencies on the underlying models of that Concept. In the example above, a model referencing `orders` Concept would depend on `stg_orders` (base) and `stg_customers` (join). The dependency is as if the model had directly `ref('stg_orders')` and `ref('stg_customers')` in its SQL (even though it didn't explicitly). This ensures the existing scheduling and ordering in `dbt run` remains correct. No separate scheduling is needed for Concepts (they are always compiled into their consumers).

  * These dependencies will appear in the manifest JSON under the model's `depends_on.nodes` list (just like multiple refs). There might also be a new section (like `depends_on.Concepts`) if we want to explicitly list Concept references for clarity, but it’s not strictly needed to execute correctly.
  * **Avoiding Cycles:** Since concepts only join to models (not other concepts), cycle detection is simplified. We only need to ensure that a concept's base model is not included in its own joins, which would create a direct self-reference.
* **Manifest Entries:** Concepts could be stored in the manifest similarly to sources or exposures. For example, `manifest.json` might have an `"Concepts"` key mapping Concept names to their parsed config. This allows `cref` to quickly retrieve definitions. It also means the manifest can be used by external tools (or docs generation) to introspect the Concept network.
* **Ephemeral vs Materialized:** By design, using an Concept does *not* create a new materialized model. It behaves conceptually like an ephemeral model defined implicitly at query time. This is fully backward-compatible: if you don't use `cref`, nothing extra runs. If you do use `cref`, the joins happen within the SQL of the model that invoked it. This avoids changing the number of nodes or the flow of execution in a run.
* **dbt Docs / Lineage Visualization:** With Concepts in play, lineage graphs could optionally show Concept references as dashed lines or annotations (though not as separate nodes). For the first implementation, we may simply show that a model depends on the base and join models (since that’s what actually runs). However, in documentation, we might list under a model: "Uses Concept: orders" for clarity. This could be a future enhancement to the docs site: indicating semantic dependencies.

By fitting into the existing graph in this manner, we achieve the goal of no new mandatory nodes and no DAG migration. Teams can incrementally adopt Concepts for new models while old models remain unchanged.

## Error Handling and Validation

Robust error handling will be implemented to ensure this feature is as safe and predictable as normal `ref` usage:

* **Undefined Concept:** If a model calls `cref('x', ...)` and there is no Concept named `x` in the project (or packages), the parser will raise a compilation error much like an undefined ref. The error will clearly state that the Concept is not found.
* **Unknown or Invalid Fields:** If the field list passed to `cref` contains a name that is not declared as a feature of that Concept (or if the field name is mistyped), compilation halts with an error. The message will indicate which field is invalid and which Concept was being used. This validation is analogous to how dbt would error if you select a column that doesn’t exist in a source table, except our check can happen at compile time via the Concept schema.
* **Ambiguous Feature Names:** If two join paths provide a feature with the same name (for example, the base model and a joined model both have a column `customer_id`), then just specifying `customer_id` could be ambiguous. Our strategy:

  * We will prefer a deterministic rule or require disambiguation. A simple rule could be “base model features take precedence unless explicitly qualified,” but this might be confusing. Instead, we may **disallow ambiguity**: the Concept YAML should not expose two features with the same final name. If it does, the parser can throw an error during Concept definition (asking the user to alias one of them via an `alias` property in the feature definition).
  * If ambiguous names slip through or if the user tries to request an ambiguous name, `cref` will error asking for clarification. We could consider supporting qualified syntax in the field list (e.g. `'customer.customer_name'` vs `'orders.customer_name'`) but that complicates the macro API. Simpler is to avoid the situation via unique feature naming.
* **Duplicate Model Joins:** If a concept definition includes multiple joins to the same model with different aliases, this could cause ambiguity. The YAML config should ideally avoid this, but if it occurs, we'll raise an error asking the user to clarify which join they want.
* **Self-Referential Models:** If a model tries to use `cref` to reference a concept that includes that model in its definition (e.g., using `cref('orders', [...])` inside the `stg_orders` model itself), this would create a cycle. This should be detected and prevented during compilation.
* **Compilation Failures:** If for some reason the `cref` macro fails to generate valid SQL (e.g. due to a bug or an edge case), it should fail clearly rather than produce incorrect SQL. We will include unit tests for various edge cases to minimize this risk. For example, if a field appears in the Concept YAML as a calculated expression that is database-specific, we ensure that expression is inserted correctly.
* **Field Name Conflicts:** If a user selects features that result in duplicate column names in the subquery (like selecting `customer_id` from both base and also as a joined field under a different name), the macro will alias one of them to avoid a SQL error. We could automatically prefix joined fields with the Concept name or alias if a conflict with base arises (similar to how dbt might handle source column collisions).
* **Deprecated/Experimental Warnings:** Initially, this feature might be marked experimental. If so, using `cref` could raise a gentle warning that this is a new feature, just to set expectations. This is optional, but if we anticipate changes, it may help.

Throughout error handling, the goal is to make error messages **clear for the end user** (analysts and engineers). For instance: "Unknown Concept 'X' referenced in model Y", "Concept 'orders' has no feature 'customer\_nme' (did you mean 'customer\_name'?)", or "Concept join path for 'region\_name' is ambiguous due to multiple region joins in 'orders' Concept."

## Testing Strategy

Implementing Concepts and `cref` touches parsing, compilation, and SQL generation. A comprehensive testing approach is required:

* **Unit Tests for Parsing:**

  * Test that YAML with various Concept definitions is parsed correctly into the internal structures. For example, ensure that joins are resolved to the correct model nodes.
  * Validate that invalid configs produce parse errors. For instance, test a YAML where a concept's base model is also listed in its joins, and confirm the parser raises an appropriate exception.
  * Test the dependency registration logic: given an Concept with multiple joins, ensure that a model containing an `cref` to that Concept ends up with the expected dependency list (e.g. check the manifest that model `depends_on` includes those models).
  * If implementing the more precise field-based dependency resolution, unit tests should cover that a static list of fields leads to exactly the correct set of dependencies.
* **Unit Tests for Macro SQL Generation:**

  * Using dbt's internal compilation testing harness (which can compile a project without running), verify that for a given `cref` call, the resulting SQL string matches expectations.
  * We can simulate a small project in tests with known models (perhaps using the SQLite adapter for simplicity) and a dummy Concept YAML. Then compile a model with an `cref` and assert the compiled SQL contains the correct `JOIN` clauses and selected columns.
  * Test variations: requesting one base field vs multiple fields vs all fields; requesting fields from two different joins simultaneously
  * Test that a field exclusively in the base produces no joins in SQL.
  * Ensure that aliasing works: if we give an alias in YAML and in field selection, the SQL uses that alias for the table.
* **Integration Tests:**

  * Set up a fake warehouse (or use a real one in a CI environment) with tables corresponding to base and joined models. For example, a small dataset for `stg_orders`, `stg_customers`, `stg_product_details`. Declare Concepts in YAML and a model selecting via `cref`. Run `dbt compile` and `dbt run`:

    * Verify that `dbt compile` succeeds and the compiled SQL is correct (no syntax errors, correct structure).
    * Verify that `dbt run` produces the expected data. For instance, compare the results of the `cref`-using model to a manually written equivalent SQL to ensure the data matches.
  * Include tests for backward compatibility: e.g., a project with no Concepts defined should run exactly as before. Possibly create two similar models, one using traditional ref + join SQL, another using `cref`, and confirm they yield the same results.
* **dbt’s own test suite:** Once implemented, the new code should be integrated into dbt-core's tests. This includes:

  * Model parsing tests (if any snapshot of manifest or node properties is checked).
  * The `dbt parser` and `dbt compiler` internal tests might need new cases to cover Concept usage.
  * If `cref` is a built-in, tests around macro context and Jinja rendering should confirm no conflicts with existing macros.
* **Edge Cases:** Write tests for known edge cases:

  * Concepts with no joins (just a base model) – does `cref` essentially just ref the base model correctly?
  * Concepts with multiple joins where none of the join fields are selected – ensure no join happens.
  * Ambiguous fields scenario – ensure it throws an error (if we simulate an ambiguous setup).
  * Large field list – if someone selects all features of an Concept, the SQL should include all joins (basically reconstructing the full unified table), test performance or at least correctness of that.
  * Interaction with other macros – ensure that using `cref` inside a CTE or alongside other Jinja logic doesn't break. (Likely fine as it returns a subquery string.)

By covering parsing, compilation, and runtime, we ensure confidence that the feature works as intended. We should leverage dbt’s robust testing frameworks, including the sample projects and the ability to run specific models, to verify this in realistic scenarios.
