# dbt-core + dbt-sqlserver, nothing else.

* This is a fork of the [dbt-core](https://github.com/dbt-labs/dbt-core) repo. I haven't done any changes to the code, but I just wanted a static repo to build dbt-sqlserver from, and push it to docker hub.
* On the docker image, I have added the dbt-sqlserver, from their [github](https://github.com/dbt-msft/dbt-sqlserver).
* Docker hub image is located [here](https://hub.docker.com/r/fyksen/dbt-sqlserver)

## How to build locally
* dbt-core repo needs docker to build. It does not run on podman on out the box, because it expects you to have buildx.

```
cd docker
sudo docker build --tag dbt-sqlserver:latest --target dbt-third-party --build-arg dbt_third_party=dbt-sqlserver .
```

## How to run

### docker:

```
docker run \
--network=host \
--mount type=bind,source=path/to/project,target=/usr/app \
--mount type=bind,source=path/to/profiles.yml,target=/root/.dbt/ \
--rm \
fyksen/dbt-sqlserver:latest \
<command>
```

### podman:

```
podman run \
--network=host \
--rm \
--mount type=bind,source=/home/user/appdata,target=/usr/app,relabel=shared \
--mount type=bind,source=/home/user/profiles.yml,target=/root/.dbt/,relabel=shared \
fyksen/dbt-mysql:latest <command>
```

In both of these configuration, the setup creates a new container for every run, and deletes the container when the command has ran. It expects a profiles.yml file and a project directory to already be created.
