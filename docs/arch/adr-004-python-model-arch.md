# Python Model Arch

## Context
We are thinking of supporting `python` ([roadmap](https://github.com/dbt-labs/dbt-core/blob/main/docs/roadmap/2022-05-dbt-a-core-story.md#scene-3-python-language-dbt-models), [discussion](https://github.com/dbt-labs/dbt-core/discussions/5261)) as a language other than SQL in dbt-core. This would allow users to express transformation logic that is tricky to do in SQL and have more libraries available to them.

### Options

#### Where to run the code
- running it locally where we run dbt core.
- running it in the cloud providers' environment.

#### What are the guardrails dbt would enforce for the python model
- None, users can write whatever code they like.
- focusing on data transformation logic where each python model should have a model function that returns a database object for dbt to materialize.

#### Where should the implementation live
Two places we need to consider are `dbt-core` and each individual adapter code-base. What are the pieces needed? How do we decide what goes where?


#### Are we going to allow writing macros in python
- Not allowing it.
- Allowing certain Jinja templating
- Allow everything

## Decisions
#### Where to run the code
In the same idea of dbt is not your query engine, we don't want dbt to be your python runtime. Instead, we want dbt to focus on being the place to express transformation logic. So python model will be following the existing pattern of the SQL model(parse and compile user written logic and submit it to your computation engine).

#### What are the guardrails dbt would enforce for the python model
We want dbt to focus on transformation logic, so we opt for setting up some tools and guardrails for the python model to focus on doing data transformation.
1. A `dbt` object would have functions including `dbt.ref`, `dbt.source` function to reference other models and sources in the dbt project, the return of the function will be a dataframe of referenced resources. 
1. Code in the python model node should include a model function that takes a `dbt` object as an argument, do the data transformation logic inside, and return a dataframe in the end. We think folks should load their data into dataframes using the `dbt.ref`, `dbt.source` provided over raw data references. We also think logic to write dataframe to database objects should live in materialization logic instead of transformation code.
1. That `dbt` object should also have an attribute called `dbt.config` to allow users to define configurations of the current python model like materialization logic, a specific version of python libraries, etc. This `dbt.config` object should also provide a clear access function for variables defined in project YAML. This way user can access arbitrary configuration at runtime.

#### Where should the implementation live

Logic in core should be universal and carry the opionions we have for the feature, this includes but not limited to
1. parsing of python file in dbt-core to get the `ref`, `source`, and `config` information. This information is used to place the python model in the correct place in project DAG and generate the correct python code sent to compute engine. 
1. `language` as a new top-level node property.
1. python template code that is not cloud provider-specific, this includes implementation for `dbt.ref`, `dbt.source`. We would use ast parser to parse out all of the `dbt.ref`, `dbt.source` inside python during parsing time, and generate what database resources those points to during compilation time. This should allow user to copy-paste the "compiled" code, and run it themselves against the data warehouse — just like with SQL models. A example of definition for `dbt.ref` could look like this
    ```python
    def ref(*args):
    refs = {"my_sql_model": "DBT_TEST.DBT_SOMESCHEMA.my_sql_model"}
    key = ".".join(args)
    return load_df_function(refs[key])
    ```

1. functional tests for the python model, these tests are expected to be inherited in the adapter code to make sure intended functions are met.
1. Generalizing the names of properties (`sql`, `raw_sql`, `compiled_sql`) for a future where it's not all SQL.
1. implementation of restrictions have for python model.


Computing engine specific logic should live in adapters, including but not limited to
- `load_df_function` of how to load a dataframe for a given database resource, 
- `materialize` of how to save a dataframe to table or other materialization formats.
- some kind of `submit_python` function for submitting python code to compute engine.
- addition or modification `materialization` macro to add materialize the python model


#### Are we going to allow writing macros in python

We don't know yet. We use macros in SQL models because it allows us to achieve what SQL can't do. But with python being a programming language, we don't see a strong need for macros in python yet. So we plan to strictly disable that in the user-written code in the beginning, and potentially add more as we hear from the community.

## Status
Implementing

# Consequences
Users would be able to write python transformation models in dbt and run them as part of their data transformation workflow.
