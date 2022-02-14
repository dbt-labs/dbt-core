# Docker for dbt
This docker file is suitable for building dbt Docker images locally or using with CI/CD to automate populating a container registry.


## Building an image:
This Dockerfile can create images for the following targets, each named after the database they support:
* `dbt-core` _(no db-adapter support)_
* `dbt-postgres`
* `dbt-redshift`
* `dbt-bigquery`
* `dbt-snowflake`
* `dbt-spark`
* `dbt-third-party` _(requires additional build-arg)_
* `dbt-all` _(installs all of the above in a single image)_

In order to build a new image, run the following docker command.
```
docker build --tag <your_image_name>  --target <target_name> <path/to/dockerfile>
```
By default the images will be populated with the most recent release of `dbt-core` and whatever database adapter you select.  If you need to use a different version you can specify it by git ref using the `--build-arg` flag:
```
docker build --tag <your_image_name> \
  --target <target_name> \
  --build-arg <arg_name>=<git_ref> \
  <path/to/dockerfile>
```
valid arg names for versioning are:
* `dbt_core_ref`
* `dbt_postgres_ref`
* `dbt_redshift_ref`
* `dbt_bigquery_ref`
* `dbt_snowflake_ref`
* `dbt_spark_ref`

> Note: Only overide a _single_ build arg for each build. Using multiple overides may lead to a non-functioning image.

If you wish to build an image with a third-party adapter you can use the `dbt-third-party` target.  This target requires you provide a path to the adapter that can be processed by `pip` by using the `dbt_third_party` build arg:
```
docker build --tag <your_image_name> \
  --target dbt-third-party \
  --build-arg dbt_third_party=<pip_parsable_install_string> \
  <path/to/dockerfile>
```

### Examples:
To build an image named "my-dbt" that supports redshift using the latest releases:
```
cd dbt-core/docker
docker build --tag my-dbt  --target dbt-redshift .
```

To build an image named "my-other-dbt" that supports bigquery using `dbt-core` version 0.21.latest and the bigquery adapter version 1.0.0b1:
```
cd dbt-core/docker
docker build \
  --tag my-other-dbt  \
  --target dbt-bigquery \
  --build-arg dbt_bigquery_ref=dbt-bigquery@v1.0.0b1 \
  --build-arg dbt_core_ref=dbt-core@0.21.latest  \
 .
```

To build an image named "my-third-party-dbt" that uses [Materilize third party adapter](https://github.com/MaterializeInc/materialize/tree/main/misc/dbt-materialize) and the latest release of `dbt-core`:
```
cd dbt-core/docker
docker build --tag my-third-party-dbt \
  --target dbt-third-party \
  --build-arg dbt_third_party=dbt-materialize \
  .
```


## Special cases
There are a few special cases worth noting:
* The `dbt-spark` database adapter comes in three different versions named `PyHive`, `ODBC`, and the default `all`.  If you wish to overide this you can use the `--build-arg` flag with the value of `dbt_spark_version=<version_name>`.  See the [docs](https://docs.getdbt.com/reference/warehouse-profiles/spark-profile) for more information.

* The `dbt-postgres` database adapter is released as part of the `dbt-core` codebase.  If you wish to overide the version used, make sure you use the gitref for `dbt-core`:
```
docker build --tag my_dbt \
  --target dbt-postgres \
  --build-arg dbt_postgres_ref=dbt-core@1.0.0b1 \
  <path/to/dockerfile> \
  ```

* If you need to build against another architecture (linux/arm64 in this example) you can overide the `build_for` build arg:
```
docker build --tag my_dbt \
  --target dbt-postgres \
  --build-arg build_for=linux/arm64 \
  <path/to/dockerfile> \
  ```
Supported architectures can be found in the python docker [dockerhub page](https://hub.docker.com/_/python).

## Running an image in a container:
The `ENTRYPOINT` for this Dockerfile is the command `dbt` so you can bind-mount your project to `/usr/app` and use dbt as normal:
```
docker run \
--network=host
--mount type=bind,source=path/to/project,target=/usr/app \
--mount type=bind,source=path/to/profiles.yml,target=/root/.dbt/ \
my-dbt \
ls
```
> Notes:
> * Bind-mount sources _must_ be an absolute path
> * You may need to make adjustments to the docker networking setting depending on the specifics of your data warehouse/database host.
