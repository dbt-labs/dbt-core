# ADBC driver repl

## Setup

Install [uv](https://docs.astral.sh/uv/). Then run

```
# Setup
uv venv .venv
.venv/bin/activate
uv pip install -r requirements.txt
```

You can get usage info with
```
# Usage guide
python3 adbc_local.py --help
```

## Configuration 

Create an `adbc_conf.toml` file in this directory that specifies where your driver `.dylib` lives, plus any `adbc` database options you want to use.

For example:

```toml
[default]
path = "/Users/YOUR_USER/repos/arrow-adbc/go/adbc/pkg/libadbc_driver_bigquery.dylib"
"adbc.bigquery.sql.project_id" = "your-project"
"adbc.bigquery.sql.dataset_id" = "your_dataset"
# ...
```
