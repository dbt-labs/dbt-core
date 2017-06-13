A small Docker image for [dbt](https://github.com/fishtown-analytics/dbt) based on [Alpine Python](https://github.com/jfloff/alpine-python).

The entrypoint wraps the `dbt` command with two added features:
  * If a `PROFILES_FILE` env is provided, the file at that location will be copied and run through `envsubst` before `dbt` is run.
  * `dbt` output will be grepped for errors (pending [better exit codes](https://github.com/analyst-collective/dbt/issues/297)).


## Usage

With a working directory like the following
```
|- dbt_project.yml
|- profiles.yml
|- models
   |- my-model.sql

```
and a `profiles.yml` profile `myprofile` that specifies `password: $DB_PASSWORD`:

```
$ docker run \
  -v .:/dbt -w /dbt \
  -e PROFILES_FILE=/dbt/profiles.yml \
  -e DB_PASSWORD=foobar \
  dbt:latest run --models my-model --profile myprofile