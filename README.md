### dbt

A data build tool


#### quick start

Install dbt to your home directory and add the command to your path with:
```bash
pip install --user dbt
echo "$PATH" | grep -q "$HOME/.local/bin" || ( echo 'export PATH="$PATH:$HOME/.local/bin"' >> ~/.bashrc && source ~/.bashrc)
```

#### configuration

Create a file `~/.dbt/profiles.yml` which specifies your target redshift credentials.
This file is placed in the home directory to avoid accidently checking your credentials into git.

```bash
mkdir -p ~/.dbt
touch ~/.dbt/profiles.yml
```

Insert the following lines into the newly created `profiles.yml` file (via `sample.profiles.yml`):

```yml
# output environments
user: # default env -- other envs can be created for different redshift instances/users/databases/etc
  outputs:
    my-redshift: # uniquely named
      type: redshift # only type supported
      host: localhost # any IP or fqdn
      port: 5439
      user: my_user
      pass: password
      dbname: dev
      schema: analyst_collective # or, choose your own schema. This is where compiled SQL files will be installed

  run-target: my-redshift # by default, run all commands against my-redshift instance
```

Next, create a `dbt_project.yml` file in the root of your source tree following sample.dbt_project.yml.

```yml
# Compile configuration
source-paths: ["model"]   # paths with source code to compile
target-path: "target"     # path for compiled code
clean-targets: ["target"] # directories removed by the clean task

# Test configuration
test-paths: ["test"]
```

Alternatively, you can use `dbt init` to quickly create a sample `dbt_project.yml` file in the current working directory.

Project configurations specified in `dbt_project.yml` will be applied on top of configurations specified in `~/.dbt/profiles.yml`

#### running

`dbt compile` to generate runnable SQL from model files

`dbt run` to run model files on the current `run-target` database

`dbt clean` to clear compiled files

`dbt init` to quickly create a sample `dbt_project.yml` file in the current working directory

`dbt debug` to view configured dbt parameters

#### contributing

To install `dbt` in develop mode, run:

```bash
virualenv env
source env/bin/activate
pip install -r requirements.txt

python setup.py develop
```

when the virtualenv is active, changes to the `dbt` source will be reflected when `dbt` is invoked from the command line
